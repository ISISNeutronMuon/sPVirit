mod protocol;

use protocol::coverage_matrix::{command_coverage, SupportLevel};

#[test]
fn protocol_matrix_has_no_unclassified_entries() {
    let mut supported = 0usize;
    let mut rejected = 0usize;
    let mut not_applicable = 0usize;

    for entry in command_coverage() {
        match entry.support {
            SupportLevel::Supported => supported += 1,
            SupportLevel::Rejected => rejected += 1,
            SupportLevel::NotApplicable => not_applicable += 1,
        }
    }

    assert_eq!(supported + rejected + not_applicable, 23);
    assert!(supported > 0, "expected at least one supported command");
    assert!(rejected > 0, "expected at least one rejected command");
}
