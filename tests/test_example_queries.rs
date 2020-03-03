/*use algebraicdb::create_with_writers;
use prettydiff::basic::DiffOp;
use prettydiff::text::diff_lines;
use std::io;
use std::net::Shutdown;
use tokio::fs;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tokio::stream::StreamExt;

#[tokio::test]
async fn test_example_queries() {
    let mut dir = fs::read_dir("test_queries/").await.unwrap();
    while let Ok(Some(entry)) = dir.next_entry().await {
        if entry.file_type().await.unwrap().is_dir() {
            let mut input_path = entry.path();
            let mut output_path = input_path.clone();

            input_path.push("input");
            output_path.push("output");

            let input = fs::read_to_string(input_path).await.unwrap();
            let output = fs::read_to_string(output_path).await.unwrap();

            run_example_query(input, output)
                .await
                .unwrap()
                .expect("Invalid query output")
        }
    }
}

async fn run_example_query(input: String, expected_output: String) -> io::Result<Result<(), ()>> {
    // use unix-pipe for communicating with database
    let (mut db_stream, mut our_stream) = UnixStream::pair()?;

    // Spawn a database
    tokio::spawn(async move {
        let (reader, writer) = db_stream.split();
        create_with_writers(reader, writer).await.unwrap();
    });

    // Write query input
    our_stream.write_all(input.as_bytes()).await?;
    our_stream.shutdown(Shutdown::Write)?;

    // Read output lines
    let (reader, _) = our_stream.split();
    let reader = BufReader::new(reader);
    let output: Vec<String> = reader.lines().collect::<Result<_, _>>().await?;
    let output = output.join("\n");

    // Check if output matches the expected
    let diff = diff_lines(&expected_output, &output);
    if diff.diff().iter().all(|i| match i {
        DiffOp::Equal(_) => true,
        _ => false,
    }) {
        Ok(Ok(()))
    } else {
        println!(
            "The following query gave an unexpected output:\n\n{}\n-- END QUERY\n",
            input
        );
        diff.prettytable();
        Ok(Err(()))
    }
}
*/
