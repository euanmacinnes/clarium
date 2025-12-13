use clarium::server::query::{parse, Command};

#[test]
fn line_comment_before_select() {
    let sql = "-- this is a comment\nSELECT 1";
    let cmd = parse(sql).expect("parse failed");
    match cmd {
        Command::Select(q) => {
            assert!(q.original_sql.to_uppercase().starts_with("SELECT"));
            assert_eq!(q.select.len(), 1);
        }
        _ => panic!("expected SELECT"),
    }
}

#[test]
fn inline_line_comment_after_token() {
    let sql = "SELECT /* keep */ 1 -- trailing comment\n";
    let cmd = parse(sql).expect("parse failed");
    match cmd {
        Command::Select(q) => {
            assert_eq!(q.select.len(), 1);
        }
        _ => panic!("expected SELECT"),
    }
}

#[test]
fn block_comment_multiline() {
    let sql = "/* leading\n block\n comment */\nSELECT 1";
    let cmd = parse(sql).expect("parse failed");
    match cmd {
        Command::Select(_) => {}
        _ => panic!("expected SELECT"),
    }
}

#[test]
fn comment_like_inside_string_literal_preserved() {
    let sql = "SELECT '-- not a comment' as t";
    let cmd = parse(sql).expect("parse failed");
    match cmd {
        Command::Select(q) => {
            assert!(q.original_sql.contains("-- not a comment"));
        }
        _ => panic!("expected SELECT"),
    }
}

#[test]
fn nested_block_comments() {
    let sql = "/* outer /* inner */ still comment */ SELECT 1";
    let cmd = parse(sql).expect("parse failed");
    match cmd {
        Command::Select(_) => {}
        _ => panic!("expected SELECT"),
    }
}
