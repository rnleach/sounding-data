use strum_macros::{AsStaticStr, EnumIter, EnumString};

/// Description of a site with a sounding.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Site {
    /// Site id, usually a 3 or 4 letter identifier (e.g. kord katl ksea).
    short_name: String,
    /// A longer, more human readable name.
    long_name: Option<String>,
    /// Any relevant notes about the site.
    notes: Option<String>,
    /// The state or providence where this location is located. This allows querying sites by what
    /// state or providence they are in.
    state: Option<StateProv>,
    /// Does this site represent a mobile unit.
    is_mobile: bool,
}

impl Site {
    /// Create a new site with the short name.
    pub fn new(short_name: &str) -> Self {
        Self {
            short_name: short_name.to_uppercase(),
            long_name: None,
            notes: None,
            state: None,
            is_mobile: false,
        }
    }

    /// Add a long name description.
    pub fn with_long_name<T>(self, long_name: T) -> Self
    where
        Option<String>: From<T>,
    {
        Self {
            long_name: Option::from(long_name),
            ..self
        }
    }

    /// Add notes to a site.
    pub fn with_notes<T>(self, notes: T) -> Self
    where
        Option<String>: From<T>,
    {
        Self {
            notes: Option::from(notes),
            ..self
        }
    }

    /// Add a state/providence association to a site
    pub fn with_state_prov<T>(self, state: T) -> Self
    where
        Option<StateProv>: From<T>,
    {
        Self {
            state: Option::from(state),
            ..self
        }
    }

    /// Set whether or not this is a mobile site.
    pub fn set_mobile(self, is_mobile: bool) -> Self {
        Self { is_mobile, ..self }
    }

    /// Get the short name, or id for this site
    pub fn short_name(&self) -> &str {
        &self.short_name
    }

    /// Get the long name for this site
    pub fn long_name(&self) -> Option<&str> {
        self.long_name.as_ref().map(|val| val.as_ref())
    }

    /// Get the notes for this site
    pub fn notes(&self) -> Option<&str> {
        self.notes.as_ref().map(|val| val.as_ref())
    }

    /// Get the state/providence for this site
    pub fn state_prov(&self) -> Option<StateProv> {
        self.state
    }

    /// Get whether or not this is a mobile site
    pub fn is_mobile(&self) -> bool {
        self.is_mobile
    }

    /// Return true if there is any missing data. It ignores the notes field since this is only
    /// rarely used.
    pub fn incomplete(&self) -> bool {
        self.long_name.is_none() || self.state.is_none()
    }
}

/// State/Providence abreviations for declaring a state in the site.
#[derive(Debug, Clone, Copy, PartialEq, Eq, EnumString, AsStaticStr, EnumIter)]
#[allow(missing_docs)]
pub enum StateProv {
    AL, // Alabama
    AK, // Alaska
    AZ, // Arizona
    AR, // Arkansas
    CA, // California
    CO, // Colorado
    CT, // Connecticut
    DE, // Delaware
    FL, // Florida
    GA, // Georgia
    HI, // Hawaii
    ID, // Idaho
    IL, // Illinois
    IN, // Indiana
    IA, // Iowa
    KS, // Kansas
    KY, // Kentucky
    LA, // Louisiana
    ME, // Maine
    MD, // Maryland
    MA, // Massachussetts
    MI, // Michigan
    MN, // Minnesota
    MS, // Mississippi
    MO, // Missouri
    MT, // Montana
    NE, // Nebraska
    NV, // Nevada
    NH, // New Hampshire
    NJ, // New Jersey
    NM, // New Mexico
    NY, // New York
    NC, // North Carolina
    ND, // North Dakota
    OH, // Ohio
    OK, // Oklahoma
    OR, // Oregon
    PA, // Pensylvania
    RI, // Rhode Island
    SC, // South Carolina
    SD, // South Dakota
    TN, // Tennessee
    TX, // Texas
    UT, // Utah
    VT, // Vermont
    VA, // Virginia
    WA, // Washington
    WV, // West Virginia
    WI, // Wisconsin
    WY, // Wyoming
    // US Commonwealth and Territories
    AS, // American Samoa
    DC, // District of Columbia
    FM, // Federated States of Micronesia
    MH, // Marshall Islands
    MP, // Northern Mariana Islands
    PW, // Palau
    PR, // Puerto Rico
    VI, // Virgin Islands
}

/*--------------------------------------------------------------------------------------------------
                                          Unit Tests
--------------------------------------------------------------------------------------------------*/
#[cfg(test)]
mod unit {
    use super::*;

    use std::str::FromStr;
    use strum::{AsStaticRef, IntoEnumIterator};

    #[test]
    fn test_site_incomplete() {
        let complete_site = Site {
            short_name: "kxly".to_owned(),
            long_name: Some("tv station".to_owned()),
            state: Some(StateProv::VI),
            notes: Some("".to_owned()),
            is_mobile: false,
        };

        let incomplete_site = Site {
            short_name: "kxly".to_owned(),
            long_name: Some("tv station".to_owned()),
            state: None,
            notes: None,
            is_mobile: false,
        };

        assert!(!complete_site.incomplete());
        assert!(incomplete_site.incomplete());
    }

    #[test]
    fn test_to_string_for_state_prov() {
        assert_eq!(StateProv::AL.as_static(), "AL");
    }

    #[test]
    fn test_from_string_for_state_prov() {
        assert_eq!(StateProv::from_str("AL").unwrap(), StateProv::AL);
    }

    #[test]
    fn round_trip_strings_for_state_prov() {
        for state_prov in StateProv::iter() {
            assert_eq!(
                StateProv::from_str(state_prov.as_static()).unwrap(),
                state_prov
            );
        }
    }
}
