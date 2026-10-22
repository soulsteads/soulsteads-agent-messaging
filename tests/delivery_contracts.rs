use soulsteads_messaging::delivery::{
    AgentId, ConversationKey, DeliveryMode, MessageKind, OutputClass, PeerOutput, PeerSend,
};

#[test]
fn agent_id_accepts_a_safe_opaque_identifier() {
    let agent = AgentId::try_from("support-bot.v2_7").expect("valid agent id");

    assert_eq!(agent.as_str(), "support-bot.v2_7");
}

#[test]
fn agent_id_rejects_empty_ambiguous_and_oversized_values() {
    for invalid in [
        "",
        "support:bot",
        "support/bot",
        "support\0bot",
        &"a".repeat(256),
    ] {
        assert!(
            AgentId::try_from(invalid).is_err(),
            "{invalid:?} must be rejected"
        );
    }
}

#[test]
fn conversation_key_roundtrips_the_exact_legacy_owner_key_with_matrix_colons() {
    let context = "!legacy:local:$root:thread";
    let key = ConversationKey::new("matrix", context).expect("valid conversation key");

    assert_eq!(key.legacy_owner_key(), format!("matrix\0{context}"));

    let restored = ConversationKey::from_legacy_owner_key(&key.legacy_owner_key())
        .expect("legacy key must parse");
    assert_eq!(restored.platform(), "matrix");
    assert_eq!(restored.context(), context);
    assert_eq!(restored.legacy_owner_key(), key.legacy_owner_key());
}

#[test]
fn conversation_key_rejects_nul_ambiguity_and_oversized_context() {
    assert!(ConversationKey::new("matrix", "room\0thread").is_err());
    assert!(ConversationKey::from_legacy_owner_key("matrix\0room\0thread").is_err());
    assert!(ConversationKey::new("matrix", &"r".repeat(4097)).is_err());
}

#[test]
fn peer_send_carries_only_recipient_idempotency_and_bounded_text() {
    let recipient = AgentId::try_from("support-bot").unwrap();
    let send = PeerSend::new(recipient.clone(), "request-42", "hello peer").unwrap();

    assert_eq!(send.recipient(), &recipient);
    assert_eq!(send.idempotency_key(), "request-42");
    assert_eq!(send.text(), "hello peer");

    let output = PeerOutput::new(OutputClass::PeerAnswer, "answer").unwrap();
    assert_eq!(output.class(), OutputClass::PeerAnswer);
    assert_eq!(output.text(), "answer");
    assert_ne!(OutputClass::PeerAnswer, OutputClass::ConversationOutput);
    assert_ne!(OutputClass::Receipt, OutputClass::Activity);
    assert_ne!(OutputClass::Presence, OutputClass::PeerAnswer);
    assert_eq!(DeliveryMode::LegacyHuman, DeliveryMode::LegacyHuman);
    assert_ne!(DeliveryMode::LegacyHuman, DeliveryMode::QueueOnly);
    assert_ne!(MessageKind::Request, MessageKind::Answer);
    assert_ne!(MessageKind::Answer, MessageKind::Information);
}

#[test]
fn peer_text_is_limited_to_64_kibibytes_for_input_and_output() {
    let recipient = AgentId::try_from("support-bot").unwrap();
    let maximum = "a".repeat(64 * 1024);
    let oversized = "a".repeat(64 * 1024 + 1);

    assert_eq!(
        PeerSend::new(recipient.clone(), "request-42", maximum.as_str())
            .unwrap()
            .text(),
        maximum
    );
    assert_eq!(
        PeerOutput::new(OutputClass::ConversationOutput, maximum.as_str())
            .unwrap()
            .text(),
        maximum
    );
    assert!(PeerSend::new(recipient, "request-42", oversized.as_str()).is_err());
    assert!(PeerOutput::new(OutputClass::ConversationOutput, oversized).is_err());
}

#[test]
fn peer_send_rejects_ambiguous_or_oversized_idempotency_keys() {
    let recipient = AgentId::try_from("support-bot").unwrap();

    assert!(PeerSend::new(recipient.clone(), "request\0route", "hello").is_err());
    assert!(PeerSend::new(recipient, "k".repeat(256), "hello").is_err());
}
