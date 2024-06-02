//! Parse (known) field names of journald-entries.
//!
//! See: [systemd.journal-fields](https://www.freedesktop.org/software/systemd/man/254/systemd.journal-fields.html)

use std::borrow::Cow;

use phf::phf_map;

static KNOWN_NAMES: phf::Map<&'static [u8], Known> = phf_map! {
    // User Fields
    // https://www.freedesktop.org/software/systemd/man/254/systemd.journal-fields.html#User%20Journal%20Fields
    b"MESSAGE" => Known::Message,
    b"MESSAGE_ID" => Known::MessageId,
    b"PRIORITY" => Known::Priority,
    b"CODE_FILE" => Known::CodeFile,
    b"CODE_LINE" => Known::CodeLine,
    b"CODE_FUNC" => Known::CodeFunc,
    b"ERRNO" => Known::Errno,
    b"INVOCATION_ID" => Known::InvocationId,
    b"USER_INVOCATION_ID" => Known::UserInvocationId,
    b"SYSLOG_FACILITY" => Known::SyslogFacility,
    b"SYSLOG_IDENTIFIER" => Known::SyslogIdentifier,
    b"SYSLOG_PID" => Known::SyslogPid,
    b"SYSLOG_TIMESTAMP" => Known::SyslogTimestamp,
    b"SYSLOG_RAW" => Known::SyslogRaw,
    b"DOCUMENTATION" => Known::Documentation,
    b"TID" => Known::Tid,
    b"UNIT" => Known::Unit,
    b"USER_UNIT" => Known::UserUnit,
    // Trusted Journal Fields
    // https://www.freedesktop.org/software/systemd/man/254/systemd.journal-fields.html#User%20Journal%20Fields
    b"_PID" => Known::_Pid,
    b"_UID" => Known::_Uid,
    b"_GID" => Known::_Gid,
    b"_COMM" => Known::_Comm,
    b"_EXE" => Known::_Exe,
    b"_CMDLINE" => Known::_Cmdline,
    b"_CAP_EFFECTIVE" => Known::_CapEffective,
    b"_AUDIT_SESSION" => Known::_AuditSession,
    b"_AUDIT_LOGINUID" => Known::_AuditLoginuid,
    b"_SYSTEMD_CGROUP" => Known::_SystemdCgroup,
    b"_SYSTEMD_SLICE" => Known::_SystemdSlice,
    b"_SYSTEMD_UNIT" => Known::_SystemdUnit,
    b"_SYSTEMD_USER_UNIT" => Known::_SystemdUserUnit,
    b"_SYSTEMD_USER_SLICE" => Known::_SystemdUserSlice,
    b"_SYSTEMD_SESSION" => Known::_SystemdSession,
    b"_SYSTEMD_OWNER_UID" => Known::_SystemdOwnerUid,
    b"_SELINUX_CONTEXT" => Known::_SelinuxContext,
    b"_SOURCE_REALTIME_TIMESTAMP" => Known::_SourceRealtimeTimestamp,

    b"_BOOT_ID" => Known::_BootId,
    b"_MACHINE_ID" => Known::_MachineId,
    b"_SYSTEMD_INVOCATION_ID" => Known::_SystemdInvocationId,
    b"_HOSTNAME" => Known::_Hostname,
    b"_TRANSPORT" => Known::_Transport,
    b"_STREAM_ID" => Known::_StreamId,
    b"_LINE_BREAK" => Known::_LineBreak,
    b"_NAMESPACE" => Known::_Namespace,
    b"_RUNTIME_SCOPE" => Known::_RuntimeScope,

    // Kernel Journal Fields
    b"_KERNEL_DEVICE" => Known::_KernelDevice,
    b"_KERNEL_SUBSYSTEM" => Known::_KernelSubsystem,
    b"_UDEV_SYSNAME" => Known::_UdevSysname,
    b"_UDEV_DEVNODE" => Known::_UdevDevnode,
    b"_UDEV_DEVLINK" => Known::_UdevDevlink,
    b"COREDUMP_UNIT" => Known::CoredumpUnit,
    b"COREDUMP_USER_UNIT" => Known::CoredumpUserUnit,
    b"OBJECT_PID" => Known::ObjectPid,
    b"OBJECT_UID" => Known::ObjectUid,
    b"OBJECT_GID" => Known::ObjectGid,
    b"OBJECT_COMM" => Known::ObjectComm,
    b"OBJECT_EXE" => Known::ObjectExe,
    b"OBJECT_CMDLINE" => Known::ObjectCmdline,
    b"OBJECT_AUDIT_SESSION" => Known::ObjectAuditSession,
    b"OBJECT_AUDIT_LOGINUID" => Known::ObjectAuditLoginuid,
    b"OBJECT_SYSTEMD_CGROUP" => Known::ObjectSystemdCgroup,
    b"OBJECT_SYSTEMD_SESSION" => Known::ObjectSystemdSession,
    b"OBJECT_SYSTEMD_OWNER_UID" => Known::ObjectSystemdOwnerUid,
    b"OBJECT_SYSTEMD_UNIT" => Known::ObjectSystemdUnit,
    b"OBJECT_SYSTEMD_USER_UNIT" => Known::ObjectSystemdUserUnit,
    b"__CURSOR" => Known::__Cursor,
    b"__REALTIME_TIMESTAMP" => Known::__RealtimeTimestamp,
    b"__MONOTONIC_TIMESTAMP" => Known::__MonotonicTimestamp,
    b"__SEQNUM" => Known::__Seqnum,
    b"__SEQNUM_ID" => Known::__SeqnumId
};

#[derive(Clone, PartialEq, Eq)]
pub enum Known {
    Message,
    MessageId,
    Priority,
    CodeFile,
    CodeLine,
    CodeFunc,
    Errno,
    InvocationId,
    UserInvocationId,
    SyslogFacility,
    SyslogIdentifier,
    SyslogPid,
    SyslogTimestamp,
    SyslogRaw,
    Documentation,
    Tid,
    Unit,
    UserUnit,
    _Pid,
    _Uid,
    _Gid,
    _Comm,
    _Exe,
    _Cmdline,
    _CapEffective,
    _AuditSession,
    _AuditLoginuid,
    _SystemdCgroup,
    _SystemdSlice,
    _SystemdUnit,
    _SystemdUserUnit,
    _SystemdUserSlice,
    _SystemdSession,
    _SystemdOwnerUid,
    _SelinuxContext,
    _SourceRealtimeTimestamp,

    _BootId,
    _MachineId,
    _SystemdInvocationId,
    _Hostname,
    _Transport,
    _StreamId,
    _LineBreak,
    _Namespace,
    _RuntimeScope,
    _KernelDevice,
    _KernelSubsystem,
    _UdevSysname,
    _UdevDevnode,
    _UdevDevlink,
    CoredumpUnit,
    CoredumpUserUnit,
    ObjectPid,
    ObjectUid,
    ObjectGid,
    ObjectComm,
    ObjectExe,
    ObjectCmdline,
    ObjectAuditSession,
    ObjectAuditLoginuid,
    ObjectSystemdCgroup,
    ObjectSystemdSession,
    ObjectSystemdOwnerUid,
    ObjectSystemdUnit,
    ObjectSystemdUserUnit,
    __Cursor,
    __RealtimeTimestamp,
    __MonotonicTimestamp,
    __Seqnum,
    __SeqnumId,
}

impl Known {
    pub fn as_bytes(&self) -> &'static [u8] {
        match self {
            // User Fields
            // https://www.freedesktop.org/software/systemd/man/254/systemd.journal-fields.html#User%20Journal%20Fields
            Known::Message => b"MESSAGE",
            Known::MessageId => b"MESSAGE_ID",
            Known::Priority => b"PRIORITY",
            Known::CodeFile => b"CODE_FILE",
            Known::CodeLine => b"CODE_LINE",
            Known::CodeFunc => b"CODE_FUNC",
            Known::Errno => b"ERRNO",
            Known::InvocationId => b"INVOCATION_ID",
            Known::UserInvocationId => b"USER_INVOCATION_ID",
            Known::SyslogFacility => b"SYSLOG_FACILITY",
            Known::SyslogIdentifier => b"SYSLOG_IDENTIFIER",
            Known::SyslogPid => b"SYSLOG_PID",
            Known::SyslogTimestamp => b"SYSLOG_TIMESTAMP",
            Known::SyslogRaw => b"SYSLOG_RAW",
            Known::Documentation => b"DOCUMENTATION",
            Known::Tid => b"TID",
            Known::Unit => b"UNIT",
            Known::UserUnit => b"USER_UNIT",
            // Trusted Journal Fields
            // https://www.freedesktop.org/software/systemd/man/254/systemd.journal-fields.html#User%20Journal%20Fields
            Known::_Pid => b"_PID",
            Known::_Uid => b"_UID",
            Known::_Gid => b"_GID",
            Known::_Comm => b"_COMM",
            Known::_Exe => b"_EXE",
            Known::_Cmdline => b"_CMDLINE",
            Known::_CapEffective => b"_CAP_EFFECTIVE",
            Known::_AuditSession => b"_AUDIT_SESSION",
            Known::_AuditLoginuid => b"_AUDIT_LOGINUID",
            Known::_SystemdCgroup => b"_SYSTEMD_CGROUP",
            Known::_SystemdSlice => b"_SYSTEMD_SLICE",
            Known::_SystemdUnit => b"_SYSTEMD_UNIT",
            Known::_SystemdUserUnit => b"_SYSTEMD_USER_UNIT",
            Known::_SystemdUserSlice => b"_SYSTEMD_USER_SLICE",
            Known::_SystemdSession => b"_SYSTEMD_SESSION",
            Known::_SystemdOwnerUid => b"_SYSTEMD_OWNER_UID",
            Known::_SelinuxContext => b"_SELINUX_CONTEXT",
            Known::_SourceRealtimeTimestamp => b"_SOURCE_REALTIME_TIMESTAMP",

            Known::_BootId => b"_BOOT_ID",
            Known::_MachineId => b"_MACHINE_ID",
            Known::_SystemdInvocationId => b"_SYSTEMD_INVOCATION_ID",
            Known::_Hostname => b"_HOSTNAME",
            Known::_Transport => b"_TRANSPORT",
            Known::_StreamId => b"_STREAM_ID",
            Known::_LineBreak => b"_LINE_BREAK",
            Known::_Namespace => b"_NAMESPACE",
            Known::_RuntimeScope => b"_RUNTIME_SCOPE",

            // Kernel Journal Fields
            Known::_KernelDevice => b"_KERNEL_DEVICE",
            Known::_KernelSubsystem => b"_KERNEL_SUBSYSTEM",
            Known::_UdevSysname => b"_UDEV_SYSNAME",
            Known::_UdevDevnode => b"_UDEV_DEVNODE",
            Known::_UdevDevlink => b"_UDEV_DEVLINK",
            Known::CoredumpUnit => b"COREDUMP_UNIT",
            Known::CoredumpUserUnit => b"COREDUMP_USER_UNIT",
            Known::ObjectPid => b"OBJECT_PID",
            Known::ObjectUid => b"OBJECT_UID",
            Known::ObjectGid => b"OBJECT_GID",
            Known::ObjectComm => b"OBJECT_COMM",
            Known::ObjectExe => b"OBJECT_EXE",
            Known::ObjectCmdline => b"OBJECT_CMDLINE",
            Known::ObjectAuditSession => b"OBJECT_AUDIT_SESSION",
            Known::ObjectAuditLoginuid => b"OBJECT_AUDIT_LOGINUID",
            Known::ObjectSystemdCgroup => b"OBJECT_SYSTEMD_CGROUP",
            Known::ObjectSystemdSession => b"OBJECT_SYSTEMD_SESSION",
            Known::ObjectSystemdOwnerUid => b"OBJECT_SYSTEMD_OWNER_UID",
            Known::ObjectSystemdUnit => b"OBJECT_SYSTEMD_UNIT",
            Known::ObjectSystemdUserUnit => b"OBJECT_SYSTEMD_USER_UNIT",
            Known::__Cursor => b"__CURSOR",
            Known::__RealtimeTimestamp => b"__REALTIME_TIMESTAMP",
            Known::__MonotonicTimestamp => b"__MONOTONIC_TIMESTAMP",
            Known::__Seqnum => b"__SEQNUM",
            Known::__SeqnumId => b"__SEQNUM_ID",
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub enum Fieldname<'a> {
    Known(Known),
    Unknown(Cow<'a, [u8]>),
}

impl<'a> From<&'a [u8]> for Fieldname<'a> {
    fn from(value: &'a [u8]) -> Self {
        if let Some(v) = KNOWN_NAMES.get(value).cloned() {
            return Fieldname::Known(v);
        }
        Fieldname::Unknown(Cow::Borrowed(value))
    }
}

impl<'a> Fieldname<'a> {
    pub fn to_owned(&self) -> Fieldname<'static> {
        match self {
            Self::Unknown(Cow::Borrowed(s)) => Fieldname::Unknown(Cow::Owned(s.to_vec())),
            Self::Unknown(Cow::Owned(s)) => Fieldname::Unknown(Cow::Owned(s.to_vec())),
            Self::Known(k) => Fieldname::Known(k.clone()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;

    use super::{Fieldname, Known};

    #[test]
    fn simple_lookup_succceeds() {
        let s = "__CURSOR".to_string();
        let f = Fieldname::from(s.as_bytes());
        assert!(f == Fieldname::Known(Known::__Cursor))
    }

    #[test]
    fn unknown_field_parsed() {
        let s = "__CURSORS".to_string();
        let f = Fieldname::from(s.as_bytes()).to_owned();
        assert!(matches!(f, Fieldname::Unknown(Cow::Owned(x)) if x == b"__CURSORS"));
    }
}
