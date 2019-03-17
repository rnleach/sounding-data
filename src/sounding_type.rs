#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SoundingType {
    observed: bool,             // False if it is a model generated sounding
    source: String,             // Description such as model name or RAWIN_SONDE
    hours_between: Option<u16>, // Hours between observations or model initializations
}

impl SoundingType {
    pub fn new<T>(src: &str, observed: bool, hours_between: T) -> Self
    where
        Option<u16>: From<T>,
    {
        SoundingType {
            observed,
            source: src.to_uppercase(),
            hours_between: Option::from(hours_between),
        }
    }

    pub fn new_model<T>(src: &str, hours_between: T) -> Self
    where
        Option<u16>: From<T>,
    {
        Self::new(src, false, hours_between)
    }

    pub fn new_observed<T>(src: &str, hours_between: T) -> Self
    where
        Option<u16>: From<T>,
    {
        Self::new(src, true, hours_between)
    }

    pub fn is_modeled(&self) -> bool {
        !self.observed
    }

    pub fn is_observed(&self) -> bool {
        self.observed
    }

    pub fn source(&self) -> &str {
        &self.source
    }

    pub fn hours_between_initializations(&self) -> Option<u16> {
        self.hours_between
    }
}
