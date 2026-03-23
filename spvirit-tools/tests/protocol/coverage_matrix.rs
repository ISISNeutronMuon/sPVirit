#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SupportLevel {
    Supported,
    Rejected,
    NotApplicable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandRole {
    ClientToServer,
    ServerToClient,
    Bidirectional,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProtocolCommandCoverage {
    pub command: u8,
    pub name: &'static str,
    pub role: CommandRole,
    pub support: SupportLevel,
    pub notes: &'static str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NtFamilyCoverage {
    pub name: &'static str,
    pub support: SupportLevel,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordTypeCoverage {
    pub name: &'static str,
    pub support: SupportLevel,
}

pub const COMMAND_COVERAGE: [ProtocolCommandCoverage; 23] = [
    ProtocolCommandCoverage {
        command: 0,
        name: "BEACON",
        role: CommandRole::ServerToClient,
        support: SupportLevel::NotApplicable,
        notes: "Server broadcast message; validated via encode/decode vector tests.",
    },
    ProtocolCommandCoverage {
        command: 1,
        name: "CONNECTION_VALIDATION",
        role: CommandRole::Bidirectional,
        support: SupportLevel::Supported,
        notes: "Full handshake supported on TCP session setup.",
    },
    ProtocolCommandCoverage {
        command: 2,
        name: "ECHO",
        role: CommandRole::Bidirectional,
        support: SupportLevel::Supported,
        notes: "Application command is echoed by server; control-plane echo is also supported.",
    },
    ProtocolCommandCoverage {
        command: 3,
        name: "SEARCH",
        role: CommandRole::ClientToServer,
        support: SupportLevel::Supported,
        notes: "UDP search request/response implemented.",
    },
    ProtocolCommandCoverage {
        command: 4,
        name: "SEARCH_RESPONSE",
        role: CommandRole::ServerToClient,
        support: SupportLevel::NotApplicable,
        notes: "Server response command; covered by codec and UDP integration tests.",
    },
    ProtocolCommandCoverage {
        command: 5,
        name: "AUTHNZ",
        role: CommandRole::Bidirectional,
        support: SupportLevel::Rejected,
        notes: "Parsed and rejected with explicit status message.",
    },
    ProtocolCommandCoverage {
        command: 6,
        name: "ACL_CHANGE",
        role: CommandRole::Bidirectional,
        support: SupportLevel::Rejected,
        notes: "Parsed and rejected with explicit status message.",
    },
    ProtocolCommandCoverage {
        command: 7,
        name: "CREATE_CHANNEL",
        role: CommandRole::Bidirectional,
        support: SupportLevel::Supported,
        notes: "Channel create response lifecycle implemented.",
    },
    ProtocolCommandCoverage {
        command: 8,
        name: "DESTROY_CHANNEL",
        role: CommandRole::Bidirectional,
        support: SupportLevel::Supported,
        notes: "Destroy request handled by SID/CID state cleanup.",
    },
    ProtocolCommandCoverage {
        command: 9,
        name: "CONNECTION_VALIDATED",
        role: CommandRole::ServerToClient,
        support: SupportLevel::NotApplicable,
        notes: "Server handshake response command.",
    },
    ProtocolCommandCoverage {
        command: 10,
        name: "GET",
        role: CommandRole::Bidirectional,
        support: SupportLevel::Supported,
        notes: "Init and data paths implemented.",
    },
    ProtocolCommandCoverage {
        command: 11,
        name: "PUT",
        role: CommandRole::Bidirectional,
        support: SupportLevel::Supported,
        notes: "Init/data/get-put paths implemented with access checks.",
    },
    ProtocolCommandCoverage {
        command: 12,
        name: "PUT_GET",
        role: CommandRole::Bidirectional,
        support: SupportLevel::Supported,
        notes: "Init and data paths implemented.",
    },
    ProtocolCommandCoverage {
        command: 13,
        name: "MONITOR",
        role: CommandRole::Bidirectional,
        support: SupportLevel::Supported,
        notes: "Init/start/stop/destroy/pipeline ack paths implemented.",
    },
    ProtocolCommandCoverage {
        command: 14,
        name: "ARRAY",
        role: CommandRole::Bidirectional,
        support: SupportLevel::Rejected,
        notes: "Parsed as op and rejected with operation status.",
    },
    ProtocolCommandCoverage {
        command: 15,
        name: "DESTROY_REQUEST",
        role: CommandRole::Bidirectional,
        support: SupportLevel::Supported,
        notes: "Monitor destroy request cleanup implemented.",
    },
    ProtocolCommandCoverage {
        command: 16,
        name: "PROCESS",
        role: CommandRole::Bidirectional,
        support: SupportLevel::Rejected,
        notes: "Parsed as op and rejected with operation status.",
    },
    ProtocolCommandCoverage {
        command: 17,
        name: "GET_FIELD",
        role: CommandRole::Bidirectional,
        support: SupportLevel::Supported,
        notes: "Channel introspection supported; list-mode enumeration is gated by server config.",
    },
    ProtocolCommandCoverage {
        command: 18,
        name: "MESSAGE",
        role: CommandRole::Bidirectional,
        support: SupportLevel::Rejected,
        notes: "Client-initiated MESSAGE is rejected.",
    },
    ProtocolCommandCoverage {
        command: 19,
        name: "MULTIPLE_DATA",
        role: CommandRole::Bidirectional,
        support: SupportLevel::Rejected,
        notes: "Parsed and rejected with explicit status message.",
    },
    ProtocolCommandCoverage {
        command: 20,
        name: "RPC",
        role: CommandRole::Bidirectional,
        support: SupportLevel::Supported,
        notes:
            "Supported for server channel list endpoint when --pvlist-mode=list; otherwise rejected.",
    },
    ProtocolCommandCoverage {
        command: 21,
        name: "CANCEL_REQUEST",
        role: CommandRole::Bidirectional,
        support: SupportLevel::Rejected,
        notes: "Parsed and rejected with explicit status message.",
    },
    ProtocolCommandCoverage {
        command: 22,
        name: "ORIGIN_TAG",
        role: CommandRole::Bidirectional,
        support: SupportLevel::Rejected,
        notes: "Parsed and rejected with explicit status message.",
    },
];

pub fn command_coverage() -> &'static [ProtocolCommandCoverage] {
    &COMMAND_COVERAGE
}

pub const NT_FAMILY_COVERAGE: [NtFamilyCoverage; 3] = [
    NtFamilyCoverage {
        name: "NTScalarArray",
        support: SupportLevel::Supported,
    },
    NtFamilyCoverage {
        name: "NTTable",
        support: SupportLevel::Supported,
    },
    NtFamilyCoverage {
        name: "NTNDArray",
        support: SupportLevel::Supported,
    },
];

pub fn nt_family_coverage() -> &'static [NtFamilyCoverage] {
    &NT_FAMILY_COVERAGE
}

pub const ARRAY_RECORD_COVERAGE: [RecordTypeCoverage; 4] = [
    RecordTypeCoverage {
        name: "waveform",
        support: SupportLevel::Supported,
    },
    RecordTypeCoverage {
        name: "aai",
        support: SupportLevel::Supported,
    },
    RecordTypeCoverage {
        name: "aao",
        support: SupportLevel::Supported,
    },
    RecordTypeCoverage {
        name: "subArray",
        support: SupportLevel::Supported,
    },
];

pub fn array_record_coverage() -> &'static [RecordTypeCoverage] {
    &ARRAY_RECORD_COVERAGE
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::{array_record_coverage, command_coverage, nt_family_coverage};

    #[test]
    fn matrix_covers_all_protocol_command_ids() {
        let entries = command_coverage();
        assert_eq!(entries.len(), 23);

        let mut ids = HashSet::new();
        for entry in entries {
            assert!(
                ids.insert(entry.command),
                "duplicate command {}",
                entry.command
            );
            assert!(entry.command <= 22);
            assert!(!entry.name.is_empty());
            assert!(!entry.notes.is_empty());
        }

        for id in 0u8..=22u8 {
            assert!(ids.contains(&id), "missing command {}", id);
        }
    }

    #[test]
    fn matrix_covers_nt_families() {
        let rows = nt_family_coverage();
        assert_eq!(rows.len(), 3);
        let names: HashSet<&str> = rows.iter().map(|r| r.name).collect();
        assert!(names.contains("NTScalarArray"));
        assert!(names.contains("NTTable"));
        assert!(names.contains("NTNDArray"));
    }

    #[test]
    fn matrix_covers_array_record_types() {
        let rows = array_record_coverage();
        assert_eq!(rows.len(), 4);
        let names: HashSet<&str> = rows.iter().map(|r| r.name).collect();
        assert!(names.contains("waveform"));
        assert!(names.contains("aai"));
        assert!(names.contains("aao"));
        assert!(names.contains("subArray"));
    }
}
