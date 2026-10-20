use super::*;
#[test]
fn rejects_unsafe_names_and_malformed_mxc() {
    for name in ["../plan.md", "x/plan.txt", "bad\0.md", ".md"] {
        assert!(!candidate(name), "unsafe name: {name:?}");
    }
    for media in [
        "mxc://hs.test/..",
        "mxc://hs.test/a b",
        "mxc://hs.test/a%2Fb",
    ] {
        assert!(
            url("https://hs.test", media).is_err(),
            "malformed media: {media}"
        );
    }
}
#[test]
fn accepts_only_markdown_and_text_extensions() {
    assert!(candidate("plan.MD"));
    assert!(candidate("note.txt"));
    assert!(!candidate("image.png"));
}
#[test]
fn validates_and_encodes_media_uri() {
    let u = url("https://hs.test/base", "mxc://media.example:8448/abc-123").unwrap();
    assert_eq!(
        u.as_str(),
        "https://hs.test/base/_matrix/client/v1/media/download/media.example:8448/abc-123"
    );
    assert!(url("https://hs.test", "mxc://x/a/b").is_err());
}
