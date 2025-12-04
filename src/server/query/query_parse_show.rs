fn parse_show(s: &str) -> Result<Command> {
    let up = s.trim().to_uppercase();
    if up == "SHOW TRANSACTION ISOLATION LEVEL" { return Ok(Command::ShowTransactionIsolation); }
    if up == "SHOW STANDARD_CONFORMING_STRINGS" { return Ok(Command::ShowStandardConformingStrings); }
    if up.starts_with("SHOW SERVER_VERSION") { return Ok(Command::ShowServerVersion); }
    if up == "SHOW CLIENT_ENCODING" { return Ok(Command::ShowClientEncoding); }
    if up == "SHOW SERVER_ENCODING" { return Ok(Command::ShowServerEncoding); }
    if up == "SHOW DATESTYLE" { return Ok(Command::ShowDateStyle); }
    if up == "SHOW INTEGER_DATETIMES" { return Ok(Command::ShowIntegerDateTimes); }
    if up == "SHOW TIME ZONE" || up == "SHOW TIMEZONE" { return Ok(Command::ShowTimeZone); }
    if up == "SHOW SEARCH_PATH" { return Ok(Command::ShowSearchPath); }
    if up == "SHOW DEFAULT_TRANSACTION_ISOLATION" { return Ok(Command::ShowDefaultTransactionIsolation); }
    if up == "SHOW TRANSACTION_READ_ONLY" { return Ok(Command::ShowTransactionReadOnly); }
    if up == "SHOW APPLICATION_NAME" { return Ok(Command::ShowApplicationName); }
    if up == "SHOW EXTRA_FLOAT_DIGITS" { return Ok(Command::ShowExtraFloatDigits); }
    if up == "SHOW ALL" { return Ok(Command::ShowAll); }
    if up.starts_with("SHOW SCHEMAS") || up.starts_with("SHOW SCHEMA") { return Ok(Command::ShowSchemas); }
    if up == "SHOW TABLES" { return Ok(Command::ShowTables); }
    if up == "SHOW OBJECTS" { return Ok(Command::ShowObjects); }
    if up == "SHOW SCRIPTS" { return Ok(Command::ShowScripts); }
    if up.starts_with("SHOW VECTOR INDEXES") { return Ok(Command::ShowVectorIndexes); }
    if up.starts_with("SHOW VECTOR INDEX ") {
        let name = s.trim()["SHOW VECTOR INDEX ".len()..].trim();
        if name.is_empty() { anyhow::bail!("SHOW VECTOR INDEX: missing name"); }
        let normalized_name = crate::ident::normalize_identifier(name);
        return Ok(Command::ShowVectorIndex { name: normalized_name });
    }
    if up.starts_with("SHOW GRAPH ") {
        let tail = s.trim()["SHOW GRAPH ".len()..].trim();
        if tail.eq_ignore_ascii_case("S") || tail.eq_ignore_ascii_case("S;") || tail.eq_ignore_ascii_case("S; ") { /* unlikely */ }
        // Accept SHOW GRAPHS and SHOW GRAPH <name>
        if tail.eq_ignore_ascii_case("S") || tail.eq_ignore_ascii_case("GRAPHS") { return Ok(Command::ShowGraphs); }
        if tail.eq_ignore_ascii_case("RAPHS") { return Ok(Command::ShowGraphs); }
        if tail.eq_ignore_ascii_case("GRAPHS;") { return Ok(Command::ShowGraphs); }
        let normalized_name = crate::ident::normalize_identifier(tail);
        return Ok(Command::ShowGraph { name: normalized_name });
    }
    if up.starts_with("SHOW GRAPHS") { return Ok(Command::ShowGraphs); }
    if up.starts_with("SHOW VIEW ") {
        let name = s.trim()["SHOW VIEW ".len()..].trim();
        if name.is_empty() { anyhow::bail!("SHOW VIEW: missing name"); }
        let normalized_name = crate::ident::normalize_identifier(name);
        return Ok(Command::ShowView { name: normalized_name });
    }
    anyhow::bail!("Unsupported SHOW command")
}