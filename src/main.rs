use std::{io::{BufReader, BufRead, Read,Write}, net::{TcpListener, TcpStream}};
use std::thread;


fn main() {
    

    let mut streams: Vec<TcpStream> = vec![];

    let listner = TcpListener::bind("127.0.0.1:8080").unwrap();

    
    for stream in listner.incoming() {
        // let mut stream  = stream.unwrap();
        let stream  = stream.unwrap();
        println!("NEW STREAM CONNECETED {stream:?}");

        // let message  = read_message(&stream);
        println!("Entered incoming()");
        // streams.push(stream);

        // println!("Message sent in the server to everyone");
        // write_message(message, &mut stream);
        
        // let streamAddr = stream.read(stream);
        // println!("New client connected {streamAddr:?}");
        // let message = stream.read().unwrap();
        // looping_old_connections(&mut streams);

        thread::spawn(move || {
            println!("{:?}", stream.peer_addr());
            looping_old_connections(stream)
        });
    }

    /*
    Why &mut *streams(deref is neccarary in the loop) is necessary in this specific loop:
    Because this code loops infinitely (loop { ... }), if you just wrote for stream in streams, Rust's ownership system would think you are trying to permanently consume or lock the streams reference on the very first iteration of the outer loop.By writing &mut *streams, you explicitly tell the compiler: "I want to create a brand new, short-lived borrow of the data inside this vector for this specific run of the for loop, and then immediately release it so the outer loop can safely run again on the next turn."
    */
    fn looping_old_connections(mut stream: TcpStream) {
        let buf_reader = BufReader::new(&stream);
        println!("This is the buf Reader {buf_reader:?}");
        println!("Inside infinite loop");
        loop {
            let message  = read_message(&buf_reader);
            write_message(message, &mut stream);
        }
    }





    // fn looping_old_connections(streams: &mut Vec<TcpStream>) {
    //     println!("Inside infinite loop");
    //     loop {
    //         for stream in &mut *streams {
    //             let message  = read_message(&stream);
    //             write_message(message, stream);
    //         }
    //     }
    // }


    // let mut looping_old_connections = || {
    //     println!("Inside infinite loop");
    //     loop {
    //         for stream in &mut *streams {
    //             let message  = read_message(&stream);
    //             write_message(message, stream);
    //         }
    //     }
    // };

    // looping_old_connections();
   

    fn read_message(val : &BufReader<TcpStream>) ->  String {
        
        // there might be problem here 
        let line = val.lines().next().unwrap().unwrap();
        println!("This is the lines inside buf reader {line:?}");
        line
    }

    fn write_message(message: String, stream: &mut TcpStream ) {
        println!("Entered inside the write function");
        stream.write_all(message.as_bytes()).unwrap();
    }
}



// fn main() {

//     let mut streams: Vec<TcpStream> = vec![];
    

//     let listner = TcpListener::bind("127.0.0.1:8080").unwrap();
//     for stream in listner.incoming() {
//         // let mut stream  = stream.unwrap();
//         let stream  = stream.unwrap();
//         println!("New stream connect {stream:?}");

//         streams.push(stream);

//         for single_stream in &streams {
//             let message  = read_message(&single_stream);
//             println!("This is the message {message}");
//             // println!("Message sent in the server to everyone");
//             // Write message should only accept one parameter that is message and in fn itself it could loop through and send it to other stream connections 
//             // write_message(message, &mut stream);
//             write_message(message);
//         }

        
//         // let streamAddr = stream.read(stream);
//         // println!("New client connected {streamAddr:?}");
//         // let message = stream.read().unwrap() ;
//     }

//     fn read_message(val : &TcpStream) ->  String {
//         let buf_reader = BufReader::new(val);
//         println!("This is the buf Reader {buf_reader:?}");
//         let line = buf_reader.lines().next().unwrap().unwrap();
//         println!("This is the lines inside buf reader {line:?}");
//         line
//     }

//     fn write_message(message: String) {
//         // for single_stream in &streams {

//         //     single_stream.write_all(message.as_bytes()).unwrap();
//         // }

//     }
//     // fn write_message(message: String, stream: &mut TcpStream ) {
//     //     stream.write_all(message.as_bytes()).unwrap();
//     // }
// }
