-- pg_available_extensions()
-- Table-valued function that lists extensions available to install.
-- Returns an array of row tables: { { name=..., default_version=..., comment=... }, ... }
-- For now, this implementation returns an empty list or a small built-in list if
-- CLARIUM_EXTENSIONS_JSON environment variable points to a JSON file describing
-- available extensions. The JSON format is an array of objects with keys
-- { name, default_version, comment }.

local json = require('json')

local function load_from_env_manifest()
  local ok, uv = pcall(function() return require('luv') end)
  -- prefer LuaFileSystem/luv if present; otherwise fall back to io.open
  local path = os.getenv('CLARIUM_EXTENSIONS_JSON')
  if not path or #path == 0 then return nil end
  -- Try read file
  local data
  if ok and uv and uv.fs_open then
    local fd = uv.fs_open(path, 'r', 438)  -- 0666
    if not fd then return nil end
    local stat = uv.fs_fstat(fd)
    data = uv.fs_read(fd, stat.size, 0)
    uv.fs_close(fd)
  else
    local f = io.open(path, 'r')
    if not f then return nil end
    data = f:read('*a')
    f:close()
  end
  if not data then return nil end
  local okj, arr = pcall(function() return json.decode(data) end)
  if not okj or type(arr) ~= 'table' then return nil end
  -- Normalize objects to expected keys
  local out = {}
  for _, e in ipairs(arr) do
    local name = e.name or e.extname or ''
    if name and #name > 0 then
      table.insert(out, {
        name = tostring(name),
        default_version = e.default_version and tostring(e.default_version) or (e.version and tostring(e.version) or nil),
        comment = e.comment and tostring(e.comment) or nil,
      })
    end
  end
  return out
end

function pg_available_extensions()
  -- Try environment-provided manifest first
  local rows = load_from_env_manifest()
  if rows and #rows > 0 then return rows end
  -- Default: return empty set; engine will LEFT JOIN with pg_extension
  return {}
end
