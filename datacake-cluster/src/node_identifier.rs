use std::str::FromStr;

use age::secrecy::ExposeSecret;
use bech32::ToBase32;
use x25519_dalek::{PublicKey, StaticSecret as SecretKey};
use zeroize::Zeroize;

/// the X25519 private key bytes as extracted from the rage keypair
pub struct ExtractedRageX25519Secret([u8; 32]);

#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NodeID(pub [u8; 62]);

/// Provides a cryptographic identifier for datacake cluster members which of
/// the age crypto system. the rage keypair identifier is used as the node identifier
/// and then broken down into the underlying X25519 secret and public keys.
///
/// This allows us to leverage both the larger dalek x25519 ecosystem of crates, while
/// utilizing rage for easy multi-member encrypted messaging
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct NodeIdentifier {
    /// the x25519 private key extracted from the rage keypair
    secret_key: [u8; 32],
    /// the secret key's corresponding public key
    public_key: [u8; 32],
    /// the node identifier which is the rage public key itself
    id: NodeID,
}

impl NodeIdentifier {
    pub fn new(id: age::x25519::Identity) -> anyhow::Result<Self> {
        Ok(TryFrom::try_from(id)?)
    }
    pub fn keypair(&self) -> (SecretKey, PublicKey) {
        let pub_key = PublicKey::from(self.public_key);
        let sec_key = SecretKey::from(self.secret_key);
        (sec_key, pub_key)
    }
    pub fn rage_id(&self) -> String {
        self.id.to_string()
    }
    pub fn id(&self) -> anyhow::Result<age::x25519::Identity> {
        ExtractedRageX25519Secret(self.secret_key).to_identity()
    }
    pub fn id_inner(&self) -> [u8; 62] {
        self.id.0
    }
    pub fn node_id(&self) -> NodeID {
        NodeID(self.id.0)
    }
}

impl FromStr for NodeID {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.len() != 62 {
            return Err(anyhow::anyhow!("incorrect data length"));
        }
        let mut buffer: [u8; 62] = [0_u8; 62];
        buffer.copy_from_slice(&s.as_bytes()[..]);
        Ok(Self(buffer))
    }
}

impl ToString for NodeID {
    fn to_string(&self) -> String {
        String::from_utf8(self.0.to_vec()).unwrap()
    }
}

impl std::fmt::Debug for NodeIdentifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.rage_id())
    }
}

impl std::fmt::Display for NodeIdentifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.rage_id())
    }
}

impl TryFrom<age::x25519::Identity> for NodeIdentifier {
    type Error = anyhow::Error;
    fn try_from(id: age::x25519::Identity) -> Result<Self, Self::Error> {
        let node_id = {
            let mut buffer: [u8; 62] = [0_u8; 62];
            let public_key = id.to_public().to_string();
            buffer.copy_from_slice(&public_key.as_bytes()[..]);
            buffer
        };
        let extracted_id = strip_rage_identity(id)?;
        let sk = SecretKey::from(extracted_id.0);
        let pk = PublicKey::from(&sk);

        Ok(Self {
            public_key: pk.to_bytes(),
            secret_key: sk.to_bytes(),
            id: NodeID(node_id),
        })
    }
}

impl TryFrom<ExtractedRageX25519Secret> for age::x25519::Identity {
    type Error = anyhow::Error;
    fn try_from(value: ExtractedRageX25519Secret) -> Result<Self, Self::Error> {
        Ok(value.to_identity()?)
    }
}

pub fn strip_rage_identity(
    id: age::x25519::Identity,
) -> anyhow::Result<ExtractedRageX25519Secret> {
    let mut debased = {
        let mut secret = id.to_string().expose_secret().clone();
        let (_, mut data, _) = bech32::decode(&secret)?;
        let debased: Vec<u8> = bech32::FromBase32::from_base32(&data)?;
        data.clear();
        secret.zeroize();
        debased
    };
    let ex_sec = {
        let mut secret_key: [u8; 32] = [0_u8; 32];
        secret_key.copy_from_slice(&debased[..]);
        debased.zeroize();
        ExtractedRageX25519Secret(secret_key)
    };

    Ok(ex_sec)
}

impl ExtractedRageX25519Secret {
    fn to_identity(&self) -> anyhow::Result<age::x25519::Identity> {
        let mut encoded = bech32::encode(
            "age-secret-key-",
            self.0.to_base32(),
            bech32::Variant::Bech32,
        )?;
        let id = age::x25519::Identity::from_str(&encoded);

        encoded.zeroize();

        match id {
            Ok(id) => Ok(id),
            Err(err) => {
                return Err(anyhow::anyhow!("failed to parse rage identity {:#?}", err))
            },
        }
    }
}

impl From<age::x25519::Recipient> for NodeID {
    fn from(r: age::x25519::Recipient) -> Self {
        let pub_key = r.to_string();
        let mut identifier: [u8; 62] = [0_u8; 62];
        identifier.copy_from_slice(&pub_key.as_bytes()[..]);
        Self(identifier)
    }
}

impl std::fmt::Debug for NodeID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.to_string())
    }
}

#[cfg(test)]
mod test {
    use age::secrecy::ExposeSecret;

    use super::*;
    #[test]
    fn test_identity() {
        let id = age::x25519::Identity::generate();

        let node_id = NodeIdentifier::new(id.clone()).unwrap();

        let parsed_id = node_id.id().unwrap();

        assert_eq!(
            id.to_string().expose_secret(),
            parsed_id.to_string().expose_secret()
        );
        let parsed_id = strip_rage_identity(id.clone()).unwrap();

        let got_id = parsed_id.to_identity().unwrap();
        assert_eq!(id.to_public().to_string(), got_id.to_public().to_string());
    }
}
