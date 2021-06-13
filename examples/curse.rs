use cursive::event::{Event, Key};
use cursive::traits::*;
use cursive::views::{Dialog, EditView, OnEventView, TextArea};
use cursive::Cursive;

use j4rs::{ClasspathEntry, InvocationArg, Jvm, JvmBuilder};
use std::convert::TryFrom;

// Just playing with a curses TUI
fn main() {
    let mut siv = cursive::default();

    // The main dialog will just have a textarea.
    // Its size expand automatically with the content.
    siv.add_layer(
        Dialog::new()
            .title("suntan")
            .padding_lrtb(1, 1, 1, 0)
            .content(TextArea::new().with_name("text"))
            .button("quit", Cursive::quit),
    );

    // We'll add a find feature!
    siv.add_layer(Dialog::info("Hint: press Ctrl-F to query!"));

    siv.add_global_callback(Event::CtrlChar('f'), |s| {
        // When Ctrl-F is pressed, show the query popup.
        // Pressing the Escape key will discard it.
        s.add_layer(
            OnEventView::new(
                Dialog::new()
                    .title("query")
                    .content(
                        EditView::new()
                            .on_submit(find)
                            .with_name("edit")
                            .min_width(10),
                    )
                    .button("Ok", |s| {
                        let text = s
                            .call_on_name("edit", |view: &mut EditView| view.get_content())
                            .unwrap();
                        find(s, &text);
                    })
                    .dismiss_button("Cancel"),
            )
            .on_event(Event::Key(Key::Esc), |s| {
                s.pop_layer();
            }),
        )
    });

    siv.run();
}

fn find(siv: &mut Cursive, text: &str) {
    // First, remove the find popup
    siv.pop_layer();

    let entry = ClasspathEntry::new("./java_wrapper/target/suntan-1.0-SNAPSHOT.jar");
    let jvm: Jvm = JvmBuilder::new().classpath_entry(entry).build().unwrap();

    // this example shard was generated with some faker data in Latin
    let instantiation_args = vec![InvocationArg::try_from("tests/resources/").unwrap()];
    let shard_reader = jvm
        .create_instance("org.suntan.ShardReader", instantiation_args.as_ref())
        .unwrap();

    let res = siv.call_on_name("text", |v: &mut TextArea| {
        let field_and_value = vec![
            InvocationArg::try_from("content").unwrap(),
            InvocationArg::try_from(text).unwrap(),
        ];

        // query the given text from the text area content
        // Possible improvement: search after the current cursor.
        match jvm
            .chain(&shard_reader)
            .unwrap()
            .invoke("queryName", &field_and_value)
        {
            Ok(java_res) => {
                let results: Vec<String> = java_res.to_rust().expect("valid rust type");
                v.set_content(results.join("\n"));
                Ok(())
            }
            Err(_) => Err(()),
        }
    });

    if let Some(Err(())) = res {
        // If we didn't find anything, tell the user!
        siv.add_layer(Dialog::info(format!("`{}` not found", text)));
    }
}
