// CNI (Container Network Interface) implementation
//
// This module provides CNI framework implementation compatible with Kubernetes conformance testing.
// It follows the CNI specification v1.0.0+ for network plugin integration.

pub mod config;
pub mod plugin;
pub mod result;
pub mod runtime;

pub use result::{CniError, ErrorCode};
pub use runtime::CniRuntime;

/// CNI specification version supported
pub const CNI_VERSION: &str = "1.0.0";

/// CNI environment variables
pub const CNI_COMMAND: &str = "CNI_COMMAND";
pub const CNI_CONTAINERID: &str = "CNI_CONTAINERID";
pub const CNI_NETNS: &str = "CNI_NETNS";
pub const CNI_IFNAME: &str = "CNI_IFNAME";
pub const CNI_ARGS: &str = "CNI_ARGS";
pub const CNI_PATH: &str = "CNI_PATH";

/// CNI commands
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CniCommand {
    Add,
    Del,
    Check,
    Version,
}

impl CniCommand {
    pub fn as_str(&self) -> &str {
        match self {
            CniCommand::Add => "ADD",
            CniCommand::Del => "DEL",
            CniCommand::Check => "CHECK",
            CniCommand::Version => "VERSION",
        }
    }
}

impl std::fmt::Display for CniCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl std::str::FromStr for CniCommand {
    type Err = CniError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "ADD" => Ok(CniCommand::Add),
            "DEL" => Ok(CniCommand::Del),
            "CHECK" => Ok(CniCommand::Check),
            "VERSION" => Ok(CniCommand::Version),
            _ => Err(CniError::new(
                ErrorCode::InvalidEnvironmentVariables,
                format!("Unknown CNI command: {}", s),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cni_command_conversion() {
        assert_eq!(CniCommand::Add.as_str(), "ADD");
        assert_eq!(CniCommand::Del.as_str(), "DEL");
        assert_eq!(CniCommand::Check.as_str(), "CHECK");
        assert_eq!(CniCommand::Version.as_str(), "VERSION");
    }

    #[test]
    fn test_cni_command_from_str() {
        assert_eq!("ADD".parse::<CniCommand>().unwrap(), CniCommand::Add);
        assert_eq!("add".parse::<CniCommand>().unwrap(), CniCommand::Add);
        assert_eq!("DEL".parse::<CniCommand>().unwrap(), CniCommand::Del);
        assert!("INVALID".parse::<CniCommand>().is_err());
    }
}
