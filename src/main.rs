use std::{collections::HashMap, io::{BufRead, BufReader, Read, Write}, net::{TcpListener, TcpStream, SocketAddr}, sync::{Arc, Mutex}};
use std::thread;


fn main() {
    
    let listener = TcpListener::bind("127.0.0.1:8080").expect("Not connected to the listener");
    // let streams: Arc<Mutex<Vec<TcpStream>>> = Arc::new(Mutex::new(vec![]));
    // let mut streams: Arc<Mutex<Vec<Arc<TcpStream>>>> = Arc::new(Mutex::new(vec![]));
    let mut streams = Arc::new(Mutex::new(vec![]));

    for stream in listener.incoming() {
        // Now i have to store the upcoming streams in global state as well as read the messages and spin a a new thread for each new upcoming connections

        // TODO DONE -- (PROVIDED OWNERSHIP TO BOTH) i have to decide whther i should give the ownership to global state variable or each new thread has the ownership of the stream 
        let stream =  Arc::new(stream.unwrap());
        // REASON OF CLONING We are cloning the the stream because we are pushing the stream into global state and then when we are spawning new thread that would also need access to the stream for reading messages, so that's why we cloned the stream
        let stream_clone = Arc::clone(&stream);
        // REASON OF CLONING we are cloning the streams first we want to push the streams in main whenever new listener comes and second we want to pass it to broadcast message, and since the global state is ARC we could have multiple owners of the same data and anyone could modify iy by cloning it.
        let streams_clone = Arc::clone(&streams);
    
        streams.lock().unwrap().push(stream);
        let length_of_streams = streams.lock().unwrap().len();

        println!("Number of conections {length_of_streams:?} ");

        thread::spawn(move || {
            println!("NEW THREAD SPAWNED");
            read_incoming_stream_messsages(stream_clone, streams_clone);
        });
    }

    fn read_incoming_stream_messsages(stream: Arc<TcpStream>, streams: Arc<Mutex<Vec<Arc<TcpStream>>>>) {
        println!("Reading messages of new connetions");

        let peer_addr = stream.peer_addr().unwrap();
        let buf_reader = BufReader::new(&*stream);
        let mut lines_iteration = buf_reader.lines();

        while let Some(Ok(message)) = lines_iteration.next() {
            println!("This is the message {message:?}");
            
            broadcast_message(message, peer_addr, Arc::clone(&streams));
        }
    }


    fn broadcast_message(message: String, peer_addr: SocketAddr, streams: Arc<Mutex<Vec<Arc<TcpStream>>>>) {
        println!("Broadcasting message");
        let streams_locked = streams.lock().expect("Streams N/A in broadcast_message");
        for  stream in  streams_locked.iter(){

            if peer_addr != stream.peer_addr().expect("Don't have the peer addr") {
                let mut raw_stream = &**stream;
                raw_stream.write_all(message.as_bytes()).unwrap();
            }
        }
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


    // fn read_message(val : &TcpStream) ->  String {
    //     let buf_reader = BufReader::new(val);
    //     println!("This is the buf Reader {buf_reader:?}");
    //     // there might be problem here 
    //     let line = buf_reader.lines().next().unwrap().unwrap();
    //     println!("This is the lines inside buf reader {line:?}");
    //     line
    // }




    /*
    Why &mut *streams(deref is neccarary in the loop) is necessary in this specific loop:
    Because this code loops infinitely (loop { ... }), if you just wrote for stream in streams, Rust's ownership system would think you are trying to permanently consume or lock the streams reference on the very first iteration of the outer loop.By writing &mut *streams, you explicitly tell the compiler: "I want to create a brand new, short-lived borrow of the data inside this vector for this specific run of the for loop, and then immediately release it so the outer loop can safely run again on the next turn."
    */
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