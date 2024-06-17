#[derive(Debug)]
pub struct JournalExportLimits {
    pub max_field_value_size: usize,
    pub max_field_name_len: usize,
    pub max_entry_size: usize,
}

impl Default for JournalExportLimits {
    fn default() -> Self {
        Self {
            max_field_value_size: 12 * 1024, // 12 KiB,
            max_field_name_len: 128,
            max_entry_size: 1 << 14,
        }
    }
}

#[derive(Default)]
pub struct JournalExportLimitsBuilder {
    max_field_value_size: Option<usize>,
    max_field_name_len: Option<usize>,
    max_entry_size: Option<usize>,
}

impl JournalExportLimitsBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_max_field_value_size(self, size: usize) -> Self {
        assert!(size > 0);
        Self {
            max_field_value_size: Some(size),
            ..self
        }
    }

    pub fn with_max_field_name_len(self, size: usize) -> Self {
        assert!(size > 0);
        Self {
            max_field_name_len: Some(size),
            ..self
        }
    }

    pub fn with_max_entry_size(self, size: usize) -> Self {
        assert!(size > 0);
        Self {
            max_entry_size: Some(size),
            ..self
        }
    }

    pub fn build(self) -> JournalExportLimits {
        let defaults = JournalExportLimits::default();
        JournalExportLimits {
            max_field_value_size: self
                .max_field_value_size
                .unwrap_or(defaults.max_field_value_size),
            max_field_name_len: self
                .max_field_name_len
                .unwrap_or(defaults.max_field_name_len),
            max_entry_size: self.max_entry_size.unwrap_or(defaults.max_entry_size),
        }
    }
}
