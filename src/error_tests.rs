use super::*;

#[test]
fn http_status_mapping() {
    assert_eq!(AppError::user("bad_input", "oops").http_status(), 400);
    assert_eq!(AppError::not_found("not_found", "missing").http_status(), 404);
    assert_eq!(AppError::conflict("conflict", "dup").http_status(), 409);
    assert_eq!(AppError::auth("auth", "no").http_status(), 401);
    assert_eq!(AppError::csrf("csrf", "blocked").http_status(), 403);
    assert_eq!(AppError::ddl("ddl_error", "bad ddl").http_status(), 400);
    assert_eq!(AppError::exec("exec_error", "fail").http_status(), 422);
    assert_eq!(AppError::io("io", "io").http_status(), 503);
    assert_eq!(AppError::internal("internal", "panic").http_status(), 500);
}

#[test]
fn pgwire_fields_mapping() {
    let (code, sev, msg) = AppError::not_found("nf", "no table").pgwire_fields();
    assert_eq!(code, "42P01");
    assert_eq!(sev, "ERROR");
    assert_eq!(msg, "no table");

    let (code, sev, _) = AppError::auth("auth", "bad").pgwire_fields();
    assert_eq!(code, "28000");
    assert_eq!(sev, "FATAL");

    let (code, sev, _) = AppError::exec("exec_error", "x").pgwire_fields();
    assert_eq!(code, "XX000");
    assert_eq!(sev, "ERROR");
}
