use std::process::Command;

#[test]
fn cli_rejects_invalid_inputs_and_non_queries() {
    for args in [
        vec!["--workers", "0", "--sql", "select 1"],
        vec!["--batch-size", "0", "--sql", "select 1"],
        vec!["--synthetic", "1000001", "--sql", "select 1"],
        vec!["--sql", "select 1; select 2"],
        vec!["--reference", "--sql", "create table side_effect (id int)"],
        vec!["--sql", "select * from missing_table"],
        vec!["--csv", "missing_separator", "--sql", "select 1"],
        vec!["--sql"],
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_ee2"))
            .args(&args)
            .output()
            .unwrap();
        assert!(!output.status.success(), "{args:?}");
        assert!(!output.stderr.is_empty(), "{args:?}");
        assert!(
            !String::from_utf8_lossy(&output.stderr).contains("panicked"),
            "{args:?}"
        );
    }
}

#[test]
fn cli_emits_csv_and_machine_metrics() {
    let output = Command::new(env!("CARGO_BIN_EXE_ee2"))
        .args([
            "--synthetic",
            "10",
            "--workers",
            "2",
            "--batch-size",
            "2",
            "--sql",
            "select category,count(*) as n from l group by category order by category",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        String::from_utf8_lossy(&output.stdout).replace('\r', ""),
        "category,n\na,5\nb,5\n"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("\"mode\":\"vectorloom\""));
    assert!(stderr.contains("\"tasks\":2"));
    assert!(stderr.contains("\"worker_ids\":[\"ThreadId("));
}
