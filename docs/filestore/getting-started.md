Clarium FILESTORE — Getting Started
==================================

This guide walks you through creating a filestore, ingesting files, listing and paging them, updating/renaming/deleting, and committing trees. All commands are executed via the SQL surface exposed by Clarium.

Prerequisites
-------------
- Server running (HTTP or pgwire). Defaults: db name "clarium", schema "public".
- Default database root created by the server on first run.

Create a filestore
------------------

Create a filestore named docs using defaults:

  CREATE FILESTORE docs;

Create with options (all fields optional):

  CREATE FILESTORE media WITH {
    "security_check_enabled": true,
    "acl_url": "https://acl.example/api/acl",
    "acl_fail_open": false,
    "git_remote": "https://github.com/example/repo.git",
    "git_branch": "main",
    "git_push_backend": "auto",
    "lfs_patterns": "*.pdf;*.pptx",
    "html_description_max_bytes": 32768
  };

Show filestores and configuration
---------------------------------

  SHOW FILESTORES;
  SHOW FILESTORE CONFIG docs;
  -- Folder override preview (no write):
  SHOW FILESTORE CONFIG docs FOLDER 'docs/handbook/';

Ingest files
------------

From raw bytes (hex or base64 supported):

  INGEST FILESTORE FILE PATH 'handbook/intro.txt'
  FROM BYTES '0x48656c6c6f2c20576f726c64210a'
  CONTENT_TYPE 'text/plain';

From a host path (requires allowlist; see concepts):

  INGEST FILESTORE FILE PATH 'slides/kickoff.pptx'
  FROM HOST_PATH 'C:\\work\\slides\\kickoff.pptx'
  CONTENT_TYPE 'application/vnd.openxmlformats-officedocument.presentationml.presentation';

List files (paged)
------------------

List all:

  SHOW FILES IN FILESTORE docs;

Filter by prefix and page with LIMIT/OFFSET:

  SHOW FILES IN FILESTORE docs LIKE 'handbook/' LIMIT 20 OFFSET 0;
  SHOW FILES IN FILESTORE docs LIKE 'handbook/' LIMIT 20 OFFSET 20;

Update, rename, delete
----------------------

Updates require an If-Match etag preflight. Fetch etag, then update from bytes:

  -- Get current metadata and etag via SHOW (or the HTTP metadata endpoint)
  SHOW FILES IN FILESTORE docs LIKE 'handbook/intro.txt';

  UPDATE FILESTORE FILE PATH 'handbook/intro.txt'
  IF_MATCH 'etag123'
  FROM BYTES 'SGVsbG8sIENsYXJpdW0h\n'
  CONTENT_TYPE 'text/plain';

Rename:

  RENAME FILESTORE FROM PATH 'handbook/intro.txt' TO PATH 'handbook/welcome.txt';

Delete (soft delete/tombstone):

  DELETE FILESTORE FILE PATH 'handbook/welcome.txt';

Versioning: trees and commits
-----------------------------

Create a tree snapshot (optionally from a logical prefix):

  CREATE TREE IN FILESTORE docs LIKE 'handbook/';

Commit the tree. If PARENTS are omitted, the current branch head is inferred.

  COMMIT TREE IN FILESTORE TREE '<tree_uuid>'
    BRANCH 'main'
    AUTHOR_NAME 'Docs Bot'
    AUTHOR_EMAIL 'docs@example'
    MESSAGE 'Publish handbook v1'
    TAGS 'docs,handbook,v1';

Show trees, commits, and diffs
------------------------------

  SHOW TREES IN FILESTORE docs;
  SHOW COMMITS IN FILESTORE docs;
  SHOW DIFF IN FILESTORE docs FROM '<parent_commit_uuid>' TO '<commit_uuid>';

Admin, health, and chunks
-------------------------

  SHOW ADMIN IN FILESTORE docs;   -- counts (files live/tomb, chunks, trees, commits)
  SHOW HEALTH IN FILESTORE docs;  -- conservative health summary
  SHOW CHUNKS IN FILESTORE docs;  -- chunk oids and sizes (internal)

Security and ACL
----------------
- Mutations call out to an external ACL service when enabled.
- Denials return clear errors. With fail-open set, transport failures allow the action but log the event.
- Correlation IDs are printed in logs as [corr=...].

Notes and best practices
------------------------
- Logical paths are UTF‑8, normalized (NFC), and slash‑separated; no empty segments.
- Content type length is capped; HTML descriptions have a configurable max size.
- SHOW paging is stable and returns an empty, typed DataFrame when OFFSET exceeds rows.
- Git push is feature‑gated; see concepts for backend selection and fallbacks.
