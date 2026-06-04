//! Convert SQL identifiers to Rust-idiomatic forms.
//!
//! The primary entry point is [`singularize`], which turns a (potentially
//! snake_case) plural noun into its singular form so that table names like
//! `users` produce `User` instead of `Users`. Rust ORMs (Diesel, SeaORM) and
//! the broader ActiveRecord-style world expect singular row structs.

/// English nouns whose plural and singular forms are identical, or that
/// happen to end in `s` but are not plural. Kept short on purpose — anything
/// off this list falls back to the regular rules.
const UNCOUNTABLES: &[&str] = &[
    "data",
    "news",
    "equipment",
    "information",
    "software",
    "statistics",
    "analytics",
    "series",
    "species",
    "means",
    "audio",
    "video",
    "metadata",
];

fn is_uncountable(word: &str) -> bool {
    let lower = word.to_ascii_lowercase();
    UNCOUNTABLES.contains(&lower.as_str())
}

/// Singularize a single English word. Conservative: when in doubt the word is
/// returned unchanged so we never butcher a perfectly good singular noun.
pub fn singularize_word(word: &str) -> String {
    if word.is_empty() || is_uncountable(word) {
        return word.to_string();
    }
    let lower = word.to_ascii_lowercase();

    // categories → category
    if lower.ends_with("ies") && word.len() > 3 {
        return format!("{}y", &word[..word.len() - 3]);
    }

    // boxes → box, churches → church, dishes → dish, classes → class
    if lower.ends_with("xes")
        || lower.ends_with("ches")
        || lower.ends_with("shes")
        || lower.ends_with("sses")
        || lower.ends_with("zes")
    {
        return word[..word.len() - 2].to_string();
    }

    // Latin imports: leave alone (analysis, basis, radius, status, virus).
    if lower.ends_with("is") || lower.ends_with("us") {
        return word.to_string();
    }

    // users → user, houses → house. Skip "address", "process" etc. (ss).
    if lower.ends_with('s') && !lower.ends_with("ss") {
        return word[..word.len() - 1].to_string();
    }

    word.to_string()
}

/// Singularize a snake_case identifier by transforming only the trailing
/// word: `user_accounts` → `user_account`, `news_posts` → `news_post`.
pub fn singularize(name: &str) -> String {
    if let Some(idx) = name.rfind('_') {
        let (prefix, last) = name.split_at(idx + 1);
        return format!("{}{}", prefix, singularize_word(last));
    }
    singularize_word(name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strips_trailing_s() {
        assert_eq!(singularize_word("users"), "user");
        assert_eq!(singularize_word("posts"), "post");
        assert_eq!(singularize_word("voices"), "voice");
    }

    #[test]
    fn handles_ies_to_y() {
        assert_eq!(singularize_word("categories"), "category");
        assert_eq!(singularize_word("queries"), "query");
        assert_eq!(singularize_word("cities"), "city");
    }

    #[test]
    fn handles_xes_ches_shes_sses() {
        assert_eq!(singularize_word("boxes"), "box");
        assert_eq!(singularize_word("churches"), "church");
        assert_eq!(singularize_word("dishes"), "dish");
        assert_eq!(singularize_word("classes"), "class");
        assert_eq!(singularize_word("addresses"), "address");
    }

    #[test]
    fn preserves_uncountables() {
        assert_eq!(singularize_word("data"), "data");
        assert_eq!(singularize_word("news"), "news");
        assert_eq!(singularize_word("series"), "series");
        assert_eq!(singularize_word("metadata"), "metadata");
    }

    #[test]
    fn preserves_latin_endings() {
        assert_eq!(singularize_word("analysis"), "analysis");
        assert_eq!(singularize_word("basis"), "basis");
        assert_eq!(singularize_word("status"), "status");
        assert_eq!(singularize_word("virus"), "virus");
    }

    #[test]
    fn preserves_double_s_words() {
        assert_eq!(singularize_word("address"), "address");
        assert_eq!(singularize_word("process"), "process");
        assert_eq!(singularize_word("class"), "class");
    }

    #[test]
    fn preserves_already_singular() {
        assert_eq!(singularize_word("user"), "user");
        assert_eq!(singularize_word("category"), "category");
        assert_eq!(singularize_word("box"), "box");
    }

    #[test]
    fn preserves_empty() {
        assert_eq!(singularize_word(""), "");
    }

    #[test]
    fn singularize_snake_case_targets_last_word() {
        assert_eq!(singularize("user_accounts"), "user_account");
        assert_eq!(singularize("agent_connector"), "agent_connector");
        assert_eq!(singularize("audit_logs"), "audit_log");
        assert_eq!(singularize("category_translations"), "category_translation");
    }

    #[test]
    fn singularize_preserves_double_underscore() {
        // The user's real-world junction table form.
        assert_eq!(singularize("agent__connector"), "agent__connector");
        assert_eq!(singularize("agent__connectors"), "agent__connector");
    }

    #[test]
    fn singularize_uppercase_unaffected_by_lower_check() {
        // We normalize to ASCII lowercase internally; original case preserved.
        assert_eq!(singularize_word("USERS"), "USER");
    }
}
