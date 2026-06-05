use std::{io::{BufReader, BufRead, Read,Write}, net::{TcpListener, TcpStream}};

fn main() {
    

    let listner = TcpListener::bind("127.0.0.1:8080").unwrap();
    for stream in listner.incoming() {
        let mut stream  = stream.unwrap();
        println!("New stream connect {stream:?}");

        let message  = read_message(&stream);

        println!("Message sent in the server to everyone");
        write_message(message, &mut stream);
        
        // let streamAddr = stream.read(stream);
        // println!("New client connected {streamAddr:?}");
        // let message = stream.read().unwrap() ;
    }

    fn read_message(val : &TcpStream) ->  String {
        let buf_reader = BufReader::new(val);
        println!("This is the buf Reader {buf_reader:?}");
        let line = buf_reader.lines().next().unwrap().unwrap();
        println!("This is the lines inside buf reader {line:?}");
        line
    }

    fn write_message(message: String, stream: &mut TcpStream ) {
        stream.write_all(message.as_bytes()).unwrap();
    }
}
