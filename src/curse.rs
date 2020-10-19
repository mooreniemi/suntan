use cursive::event::{Event, Key};
use cursive::traits::*;
use cursive::views::{Dialog, EditView, OnEventView, TextArea};
use cursive::Cursive;

use j4rs::{InvocationArg, Jvm, JvmBuilder, ClasspathEntry};
use std::convert::TryFrom;

fn main() {
    // Create a JVM
    let entry = ClasspathEntry::new("/home/alex/git/lucky-java/target/lucky-java-1.0-SNAPSHOT.jar");
    let jvm: Jvm = JvmBuilder::new()
        .classpath_entry(entry)
        .build().unwrap();

    let _static_invocation_result = jvm.invoke_static(
        "org.lucky.ShardReader",     // The Java class to invoke
        "docCount",    // The static method of the Java class to invoke
        &vec![InvocationArg::try_from("/var/lib/elasticsearch/nodes/0/indices/9XubvRRIQjC10BJKHhG1ug/0/index/").unwrap()],
    ).unwrap();

    let doc_count: isize = jvm.to_rust(_static_invocation_result).unwrap();
    println!("On load, testing, found {} docs...", doc_count);

    let mut siv = cursive::default();

    // The main dialog will just have a textarea.
    // Its size expand automatically with the content.
    siv.add_layer(
        Dialog::new()
        .title("lucky")
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
                        .call_on_name("edit", |view: &mut EditView| {
                            view.get_content()
                        })
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

    let entry = ClasspathEntry::new("/home/alex/git/lucky-java/target/lucky-java-1.0-SNAPSHOT.jar");
    let jvm: Jvm = JvmBuilder::new()
        .classpath_entry(entry)
        .build().unwrap();

    let res = siv.call_on_name("text", |v: &mut TextArea| {
        // query the given text from the text area content
        // Possible improvement: search after the current cursor.
        let java_res = jvm.invoke_static(
            "org.lucky.ShardReader",     // The Java class to invoke
            "queryName",    // The static method of the Java class to invoke
            &vec![InvocationArg::try_from(text).unwrap()],
        );

        if let Result::Ok(i) = java_res {
            let names: Vec<String> = jvm.to_rust(i).unwrap();
            v.set_content(names.join("\n"));
            Ok(())
        } else {
            // Otherwise, return an error so we can show a warning.
            Err(())
        }
    });

    if let Some(Err(())) = res {
        // If we didn't find anything, tell the user!
        siv.add_layer(Dialog::info(format!("`{}` not found", text)));
    }
}
