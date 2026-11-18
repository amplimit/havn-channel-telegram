//! The agent's Markdown as the HTML Telegram takes (`parse_mode: "HTML"`,
//! issue #11): bold, italic, strikethrough, code, code blocks, links and
//! block quotes become tags; headings become bold lines, lists keep
//! their markers and tables become a monospaced block, as Telegram has no
//! tags for them. Everything else is escaped text, so nothing the agent
//! writes is taken for a tag.

use std::fmt::Write as _;

use pulldown_cmark::{CodeBlockKind, Event, Options, Parser, Tag, TagEnd};

use crate::telegram::split_text;

/// `markdown` as Telegram HTML.
pub fn to_html(markdown: &str) -> String {
    let options = Options::ENABLE_TABLES
        | Options::ENABLE_STRIKETHROUGH
        | Options::ENABLE_TASKLISTS
        // A single `~` (as in "3~5 days") is read as a subscript and
        // written back as it was, so only `~~` strikes through.
        | Options::ENABLE_SUBSCRIPT;
    let mut writer = Writer::default();
    for event in Parser::new_ext(markdown, options) {
        writer.event(event);
    }
    let out = writer.out.trim_end().to_string();
    if out.is_empty() && !markdown.trim().is_empty() {
        return escape(markdown.trim());
    }
    out
}

/// `markdown` in parts of at most `max` UTF-16 units, as
/// [`split_text`] cuts it; a code block cut in two is closed at the end
/// of its first part and opened again, with its language, at the start of
/// the next.
pub fn split(markdown: &str, max: usize) -> Vec<String> {
    let mut parts = split_text(markdown, max);
    let mut open: Option<String> = None;
    for part in &mut parts {
        if let Some(fence) = &open {
            *part = format!("{fence}\n{part}");
        }
        open = open_fence(part);
        if let Some(fence) = &open {
            let marker: String = fence
                .trim_start()
                .chars()
                .take_while(|c| *c == '`' || *c == '~')
                .collect();
            part.push('\n');
            part.push_str(&marker);
        }
    }
    parts
}

/// The opening fence of the code block `markdown` leaves open, if any:
/// as the parser reads it, so inline code and indented text are not taken
/// for one.
fn open_fence(markdown: &str) -> Option<String> {
    let mut last = None;
    for (event, range) in Parser::new_ext(markdown, Options::empty()).into_offset_iter() {
        if let Event::Start(Tag::CodeBlock(CodeBlockKind::Fenced(_))) = event {
            last = Some(range);
        }
    }
    let block = &markdown[last?];
    let opener = block.lines().next()?.trim_start();
    let mark = opener.chars().next()?;
    let run = opener.chars().take_while(|c| *c == mark).count();
    let closed = block.trim_end().lines().skip(1).last().is_some_and(|line| {
        let fence = line
            .trim_start_matches(|c: char| c == '>' || c.is_whitespace())
            .trim_end();
        fence.chars().count() >= run && fence.chars().all(|c| c == mark)
    });
    (!closed).then(|| opener.to_string())
}

/// The UTF-16 units Telegram counts in `html`: its text, without tags,
/// with each entity as the character it stands for.
pub fn visible_units(html: &str) -> usize {
    let mut units = 0;
    let mut chars = html.chars();
    while let Some(c) = chars.next() {
        match c {
            '<' => {
                for c in chars.by_ref() {
                    if c == '>' {
                        break;
                    }
                }
            }
            '&' => {
                for c in chars.by_ref() {
                    if c == ';' {
                        break;
                    }
                }
                units += 1;
            }
            _ => units += c.len_utf16(),
        }
    }
    units
}

/// `text` with the characters HTML gives a meaning escaped.
pub fn escape(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for c in text.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            _ => out.push(c),
        }
    }
    out
}

/// Link schemes Telegram follows; a link to anything else is shown as
/// text with its address after it.
fn linkable(url: &str) -> bool {
    let lower = url.to_ascii_lowercase();
    ["http://", "https://", "tg://", "mailto:"]
        .iter()
        .any(|scheme| lower.starts_with(scheme))
}

#[derive(Default)]
struct Table {
    rows: Vec<Vec<String>>,
    cell: String,
}

struct Link {
    url: String,
    /// Written as a tag; else as text with its address after it.
    tag: bool,
    /// Where its text starts, to tell a link with none.
    at: usize,
}

#[derive(Default)]
struct Writer {
    out: String,
    /// The lists the text is in, innermost last: the next number of an
    /// ordered one.
    lists: Vec<Option<u64>>,
    /// The links the text is in, innermost last.
    links: Vec<Link>,
    table: Option<Table>,
    quotes: usize,
    code: bool,
    /// A block or item has just been opened: its first block needs no
    /// break before it.
    fresh: bool,
}

impl Writer {
    fn event(&mut self, event: Event<'_>) {
        match event {
            Event::Start(tag) => self.start(tag),
            Event::End(tag) => self.end(tag),
            Event::Text(text) => self.text(&text),
            Event::Code(code) => {
                if let Some(table) = &mut self.table {
                    table.cell.push_str(&code);
                } else {
                    self.out.push_str("<code>");
                    self.out.push_str(&escape(&code));
                    self.out.push_str("</code>");
                }
            }
            Event::InlineMath(text) | Event::Html(text) | Event::InlineHtml(text) => {
                self.text(&text);
            }
            Event::DisplayMath(text) => {
                self.block();
                self.text(&text);
            }
            Event::FootnoteReference(name) => self.text(&format!("[^{name}]")),
            Event::SoftBreak | Event::HardBreak => {
                if let Some(table) = &mut self.table {
                    table.cell.push(' ');
                } else {
                    self.out.push('\n');
                }
            }
            Event::Rule => {
                self.block();
                self.out.push_str("———");
            }
            Event::TaskListMarker(done) => self.out.push_str(if done { "[x] " } else { "[ ] " }),
        }
    }

    fn start(&mut self, tag: Tag<'_>) {
        match tag {
            Tag::Paragraph | Tag::HtmlBlock => self.block(),
            Tag::Heading { .. } => {
                self.block();
                self.out.push_str("<b>");
            }
            Tag::BlockQuote(_) => {
                self.block();
                if self.quotes == 0 {
                    self.out.push_str("<blockquote>");
                    self.fresh = true;
                }
                self.quotes += 1;
            }
            Tag::CodeBlock(kind) => {
                self.block();
                let language = match kind {
                    CodeBlockKind::Fenced(info) => info
                        .split_whitespace()
                        .next()
                        .unwrap_or_default()
                        .chars()
                        .filter(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '+' | '-' | '#'))
                        .collect::<String>(),
                    CodeBlockKind::Indented => String::new(),
                };
                self.out.push_str("<pre>");
                if language.is_empty() {
                    self.out.push_str("<code>");
                } else {
                    self.out.push_str("<code class=\"language-");
                    self.out.push_str(&language);
                    self.out.push_str("\">");
                }
                self.code = true;
            }
            Tag::List(start) => {
                if self.lists.is_empty() {
                    self.block();
                }
                self.lists.push(start);
            }
            Tag::Item => {
                if !self.fresh && !self.out.is_empty() && !self.out.ends_with('\n') {
                    self.out.push('\n');
                }
                let depth = self.lists.len().saturating_sub(1);
                self.out.push_str(&"  ".repeat(depth));
                match self.lists.last_mut() {
                    Some(Some(n)) => {
                        let _ = write!(self.out, "{n}. ");
                        *n += 1;
                    }
                    _ => self.out.push_str("• "),
                }
                self.fresh = true;
            }
            Tag::Table(_) => {
                self.block();
                self.table = Some(Table::default());
            }
            Tag::TableRow | Tag::TableHead => {
                if let Some(table) = &mut self.table {
                    table.rows.push(Vec::new());
                }
            }
            Tag::Emphasis => self.tag("<i>"),
            Tag::Strong => self.tag("<b>"),
            Tag::Strikethrough => self.tag("<s>"),
            Tag::Subscript => self.text("~"),
            Tag::Superscript => self.text("^"),
            Tag::Link { dest_url, .. } | Tag::Image { dest_url, .. } => {
                // Not a link in a link (an image in one): Telegram takes
                // no nesting of them.
                let tag = self.table.is_none()
                    && linkable(&dest_url)
                    && !self.links.iter().any(|l| l.tag);
                if tag {
                    self.out.push_str("<a href=\"");
                    self.out.push_str(&escape(&dest_url));
                    self.out.push_str("\">");
                }
                let at = self.written();
                self.links.push(Link {
                    url: dest_url.to_string(),
                    tag,
                    at,
                });
            }
            _ => {}
        }
    }

    fn end(&mut self, tag: TagEnd) {
        match tag {
            TagEnd::Heading(_) => self.out.push_str("</b>"),
            TagEnd::BlockQuote(_) => {
                self.quotes = self.quotes.saturating_sub(1);
                if self.quotes == 0 {
                    self.trim_breaks();
                    self.out.push_str("</blockquote>");
                }
            }
            TagEnd::CodeBlock => {
                self.trim_breaks();
                self.out.push_str("</code></pre>");
                self.code = false;
            }
            TagEnd::List(_) => {
                self.lists.pop();
            }
            TagEnd::Item => self.fresh = false,
            TagEnd::TableCell => {
                if let Some(table) = &mut self.table {
                    let cell = std::mem::take(&mut table.cell);
                    if let Some(row) = table.rows.last_mut() {
                        row.push(cell.trim().to_string());
                    }
                }
            }
            TagEnd::Table => {
                if let Some(table) = self.table.take() {
                    self.out.push_str("<pre>");
                    self.out.push_str(&escape(&table_text(&table.rows)));
                    self.out.push_str("</pre>");
                }
            }
            TagEnd::Emphasis => self.tag("</i>"),
            TagEnd::Strong => self.tag("</b>"),
            TagEnd::Strikethrough => self.tag("</s>"),
            TagEnd::Subscript => self.text("~"),
            TagEnd::Superscript => self.text("^"),
            TagEnd::Link | TagEnd::Image => {
                if let Some(link) = self.links.pop() {
                    // A link without text shows its address.
                    let bare = self.written() == link.at;
                    if link.tag {
                        if bare {
                            self.out.push_str(&escape(&link.url));
                        }
                        self.out.push_str("</a>");
                    } else if bare {
                        self.text(&link.url);
                    } else if !link.url.is_empty() {
                        self.text(&format!(" ({})", link.url));
                    }
                }
            }
            _ => {}
        }
    }

    /// How much has been written where text goes now.
    fn written(&self) -> usize {
        self.table
            .as_ref()
            .map_or(self.out.len(), |table| table.cell.len())
    }

    fn text(&mut self, text: &str) {
        if let Some(table) = &mut self.table {
            table.cell.push_str(text);
        } else {
            self.fresh = false;
            self.out.push_str(&escape(text));
        }
    }

    /// A tag, outside a table (whose block takes no tags).
    fn tag(&mut self, tag: &str) {
        if self.table.is_none() {
            self.out.push_str(tag);
        }
    }

    /// The break before a block: none first in a block or item, a line in
    /// a list, else a blank line.
    fn block(&mut self) {
        if self.fresh || self.code {
            self.fresh = false;
            return;
        }
        if self.out.is_empty() || self.out.ends_with("<blockquote>") {
            return;
        }
        self.trim_breaks();
        self.out
            .push_str(if self.lists.is_empty() { "\n\n" } else { "\n" });
    }

    fn trim_breaks(&mut self) {
        let kept = self.out.trim_end_matches(['\n', ' ']).len();
        self.out.truncate(kept);
    }
}

/// `rows` as lines of columns padded to one width, the header ruled off.
fn table_text(rows: &[Vec<String>]) -> String {
    let columns = rows.iter().map(Vec::len).max().unwrap_or(0);
    let widths: Vec<usize> = (0..columns)
        .map(|c| {
            rows.iter()
                .filter_map(|r| r.get(c))
                .map(|cell| cell.chars().count())
                .max()
                .unwrap_or(0)
        })
        .collect();
    let line = |row: &Vec<String>| -> String {
        (0..columns)
            .map(|c| {
                let cell = row.get(c).map_or("", String::as_str);
                format!("{cell}{}", " ".repeat(widths[c] - cell.chars().count()))
            })
            .collect::<Vec<_>>()
            .join(" | ")
            .trim_end()
            .to_string()
    };
    let mut lines: Vec<String> = Vec::with_capacity(rows.len() + 1);
    for (n, row) in rows.iter().enumerate() {
        lines.push(line(row));
        if n == 0 && rows.len() > 1 {
            lines.push(
                widths
                    .iter()
                    .map(|w| "-".repeat(*w))
                    .collect::<Vec<_>>()
                    .join("-|-"),
            );
        }
    }
    lines.join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inline_formatting_becomes_tags() {
        assert_eq!(
            to_html("**Waymo** is *here*, ~~gone~~ and `code`."),
            "<b>Waymo</b> is <i>here</i>, <s>gone</s> and <code>code</code>."
        );
    }

    #[test]
    fn what_is_not_markdown_is_escaped() {
        assert_eq!(
            to_html("a < b && c > d, <b>not a tag</b>"),
            "a &lt; b &amp;&amp; c &gt; d, &lt;b&gt;not a tag&lt;/b&gt;"
        );
        assert_eq!(
            to_html("2 * 3 = 6, and a lone * here"),
            "2 * 3 = 6, and a lone * here"
        );
    }

    #[test]
    fn a_single_tilde_stays() {
        assert_eq!(to_html("约3~5天，或者6~7天"), "约3~5天，或者6~7天");
        assert_eq!(to_html("~~struck~~"), "<s>struck</s>");
    }

    #[test]
    fn code_blocks_keep_their_language_and_text() {
        assert_eq!(
            to_html("Run:\n\n```rust\nfn main() { a < b }\n```\n\nDone."),
            "Run:\n\n<pre><code class=\"language-rust\">fn main() { a &lt; b }</code></pre>\n\nDone."
        );
        assert_eq!(to_html("```\nplain\n```"), "<pre><code>plain</code></pre>");
        assert_eq!(
            to_html("```\"><script>\nx\n```"),
            "<pre><code class=\"language-script\">x</code></pre>"
        );
    }

    #[test]
    fn headings_are_bold_and_lists_keep_markers() {
        assert_eq!(
            to_html("# Title\n\n- one\n- two\n  - deeper\n\n1. first\n2. second"),
            "<b>Title</b>\n\n• one\n• two\n  • deeper\n\n1. first\n2. second"
        );
        assert_eq!(to_html("3. three\n4. four"), "3. three\n4. four");
        assert_eq!(to_html("- [x] done\n- [ ] todo"), "• [x] done\n• [ ] todo");
    }

    #[test]
    fn loose_lists_keep_their_items_together() {
        assert_eq!(to_html("- one\n\n- two"), "• one\n• two");
    }

    #[test]
    fn links_are_tags_only_for_schemes_telegram_follows() {
        assert_eq!(
            to_html("[docs](https://example.org/a?b=1&c=2)"),
            "<a href=\"https://example.org/a?b=1&amp;c=2\">docs</a>"
        );
        assert_eq!(
            to_html("[run](javascript:alert(1))"),
            "run (javascript:alert(1))"
        );
        assert_eq!(
            to_html("![a chart](https://example.org/c.png)"),
            "<a href=\"https://example.org/c.png\">a chart</a>"
        );
    }

    #[test]
    fn a_link_without_text_shows_its_address() {
        assert_eq!(
            to_html("![](https://x.org/c.png)"),
            "<a href=\"https://x.org/c.png\">https://x.org/c.png</a>"
        );
        assert_eq!(to_html("[](ftp://x.org)"), "ftp://x.org");
        assert_eq!(
            to_html("[![badge](https://x.org/b.svg)](https://x.org)"),
            "<a href=\"https://x.org\">badge (https://x.org/b.svg)</a>"
        );
    }

    #[test]
    fn only_real_code_blocks_are_fences() {
        assert_eq!(open_fence("```foo``` bar\nmore"), None);
        assert_eq!(open_fence("    ```\n    indented"), None);
        assert_eq!(
            open_fence("text\n```rust\nlet x = 1;\n"),
            Some("```rust".into())
        );
        assert_eq!(open_fence("```rust\nlet x = 1;\n```\n"), None);
        assert_eq!(open_fence("~~~~\na\n~~~\nb"), Some("~~~~".into()));
        assert_eq!(open_fence("> ```\n> quoted\n> ```"), None);
    }

    #[test]
    fn quotes_are_one_block() {
        assert_eq!(
            to_html("> said\n>\n> > nested\n\nafter"),
            "<blockquote>said\n\nnested</blockquote>\n\nafter"
        );
    }

    #[test]
    fn tables_become_monospaced_text() {
        assert_eq!(
            to_html("| Name | Qty |\n|---|---|\n| **apple** | 3 |\n| fig | 12 |"),
            "<pre>Name  | Qty\n------|----\napple | 3\nfig   | 12</pre>"
        );
    }

    #[test]
    fn line_breaks_in_a_paragraph_are_kept() {
        assert_eq!(to_html("one\ntwo"), "one\ntwo");
    }

    #[test]
    fn raw_html_is_text() {
        assert_eq!(to_html("<div>x</div>"), "&lt;div&gt;x&lt;/div&gt;");
    }

    #[test]
    fn visible_units_count_text_only() {
        assert_eq!(visible_units("<b>ab</b> &lt;c&gt;"), 6);
        assert_eq!(visible_units(&to_html("**季度**")), 2);
    }

    #[test]
    fn a_code_block_split_in_two_stays_code() {
        let mut code = String::new();
        for i in 0..40 {
            let _ = writeln!(code, "line {i}");
        }
        let markdown = format!("intro\n\n```python\n{code}```\n\nafter");
        let parts = split(&markdown, 200);
        assert!(parts.len() > 1, "{parts:?}");
        for part in &parts {
            assert!(open_fence(part).is_none(), "left open: {part:?}");
            let html = to_html(part);
            assert!(!html.contains("```"), "{html:?}");
        }
        assert!(parts[1].starts_with("```python\n"), "{:?}", parts[1]);
        let joined: String = parts.iter().map(|p| to_html(p)).collect();
        for i in 0..40 {
            assert!(joined.contains(&format!("line {i}")), "line {i} lost");
        }
    }

    #[test]
    fn text_without_code_splits_as_before() {
        let text = "word ".repeat(2000);
        assert_eq!(split(&text, 4096), split_text(&text, 4096));
    }
}
