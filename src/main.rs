use std::{collections::HashMap, fmt::format, io::{BufRead, BufReader, Read, Write}, net::{SocketAddr, TcpListener, TcpStream}, sync::{Arc, Mutex}};
use std::thread;


fn main() {
    
    let listener = TcpListener::bind("127.0.0.1:8080").expect("Not connected to the listener");
    // let streams = Arc::new(Mutex::new(vec![]));
    // TODO (DONE) -- Now i have to store the upcoming streams in global state as well as read the messages and spin a a new thread for each new upcoming connections
    // let mut streams: Arc<Mutex<Vec<Arc<TcpStream>>>> = Arc::new(Mutex::new(vec![]));
    // TODO (DONE) -- Add another data type that stores name as well as the stream
    // let streams_new_type: Arc<Mutex<HashMap<String, TcpStream>>> = Arc::new(Mutex::new(HashMap::new()));
    let streams_new_type: Arc<Mutex<HashMap<String, Arc<TcpStream>>>> = Arc::new(Mutex::new(HashMap::new()));


    for stream in listener.incoming() {

        // TODO DONE -- (PROVIDED OWNERSHIP TO BOTH) i have to decide whther i should give the ownership to global state variable or each new thread has the ownership of the stream 
        let stream =  Arc::new(stream.unwrap());
        // REASON OF CLONING We are cloning the the stream because we are pushing the stream into global state and then when we are spawning thread that would also need access to the stream for reading messages, so that's why we cloned the stream in which we first stream could be pushed in the global state and second stream could be passed in the "read_incoming_stream_messsages" function 
        let stream_clone = Arc::clone(&stream);
        let stream_clone_1 = Arc::clone(&stream);
        
        // REASON OF CLONING we are cloning the streams first we want to push the streams in main whenever new listener comes and second we want to pass it to broadcast message, and since the global state is ARC we could have multiple owners of the same data and anyone could modify iy by cloning it.
        let streamS_clone = Arc::clone(&streams_new_type);

        // THIS THE THREAD FOR ASKING USERNAME, READING AND BROADCASTING MESSAGES
        thread::spawn(move || {
            println!("THREAD 1 SPAWNED");

            let peer_Addr = stream_clone_1.peer_addr().unwrap();
            println!("Peer CONNECTED {peer_Addr:?}");
            let mut raw_stream = &*stream_clone_1;
            raw_stream.write_all((String::from("Please enter your username ")).as_bytes()).unwrap();
            let buf_reader  = BufReader::new(raw_stream);
            let name  = buf_reader.lines().next().expect("User didn't enter username").unwrap();
            // REASON I have cloned the name cause we are passing the value of string in global state variable at "streams_new_type" but we also need the name for reading messages so that's why i cloned it below
            let name_1 = name.clone();
            println!("This is the name {name:?} ");
            streamS_clone.lock().unwrap().insert(String::from(name), stream_clone_1);

            let total_connections = streamS_clone.lock().unwrap().len();
            println!("Total NUMBER OF CONNECTIONS: {total_connections}");

            read_incoming_stream_messsages(name_1, stream_clone, streamS_clone);
        });
    }

    fn read_incoming_stream_messsages(username: String, stream: Arc<TcpStream>, streams: Arc<Mutex<HashMap<String, Arc<TcpStream>>>>) {
        println!("Reading messages of new connetions");

        let peer_addr = stream.peer_addr().unwrap();
        let buf_reader = BufReader::new(&*stream);
        let mut lines_iteration = buf_reader.lines();
        loop {
            match lines_iteration.next() {
                Some(Ok(message)) => {
                    println!("This is the message {message:?}");
                    // ASK AI -  See i am using clone for cloning the username is it efficient way to do it or can we do it more efficiently,?? FIRST SEE if deep copy is expensive or not cause we are making a new everytime a new message is broadcasted 
                    // ANSWER AI - The cost of cloning a short username like: "Alice" is negligible. So no worries we can clone the username
                    broadcast_message(username.clone(), message, Arc::clone(&streams));
                },
                None => {
                    // TODO (DONE) - here i have to implment that when a stream becomes inactive it should reflect globally so it doesn't write to that peeraddr in broadcast function
                    // ASK AI -- why is taking &username and not the only username 
                    streams.lock().unwrap().remove(&username).expect("Not able find the username");
                    println!("User {username} DISCONNECTED");
                    println!("Peer DISCONNECTED {peer_addr:?}");
                    break;
                },
                Some(Err(e)) => {
                    println!("Got an error from a peer {peer_addr:?} Error: {e:?}");
                    break;
                }
            }
        }
    }

    
    // Here in the broadcast function add a if and else that if the string of user is not present don't broadcasrt the message
    fn broadcast_message(username: String , message: String, streams_with_username: Arc<Mutex<HashMap<String, Arc<TcpStream>>>>) {
        println!("Broadcasting message");
        let streams_locked = streams_with_username.lock().expect("Streams N/A in broadcast_message");
        for  (key, value) in  streams_locked.iter(){
            println!("This is the key viz username {key} ");
            if key != &username {
                
                // println!("This is the stream {:?}", stream);
                // TODO (DONE)- Write messages to all the clients those who have active streams
                // TODO (DONE) - write messages with who sent it 
                // ASK AI - see i am using clone is that an efficient way to do it or is there a better way to do it.
                // ANSWER AI - The cost of cloning a short username like: "Alice" is negligible. So no worries we can clone the username.
                // let framed_message = username.clone() + ": " + &message ;
                let framed_message = format!("\r\n{}: {} \r\n", username, message);

                // MEANING OF &**value -- value ──▶ &Arc<TcpStream> ──▶ Arc<TcpStream> ──▶ TcpStream ──▶ &TcpStream
                (&**value).write_all(framed_message.as_bytes()).unwrap();
                /*
                    &**value -- value ──▶ &Arc<TcpStream> ──▶ Arc<TcpStream> ──▶ TcpStream ──▶ &TcpStream
                    Now here's the important part
    
                    Ask yourself:
    
                    Did we actually create a new TcpStream?
    
                    No.
    
                    We merely followed pointers to reach the existing TcpStream.
                    So when you write:
                    
                    **value
                    
                    Rust sees the value behind the pointers.
                    
                    ===============
                    Now think about write_all
    
                    Its signature is essentially:
    
                    fn write_all(&mut self, ...)
    
                    Notice something?
    
                    It doesn't consume a TcpStream.
    
                    It expects a reference.
    
                    Not the value itself.

                    ===============
                    So why &?

                    When you write:

                    &**value

                    you're saying:

                    "Go all the way to the TcpStream, then borrow it."

                    You're not creating another TcpStream.

                    You're creating a reference to the one you just reached.
                 */
                
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






    // fn read_incoming_stream_messsages(stream: Arc<TcpStream>, streams: Arc<Mutex<Vec<Arc<TcpStream>>>>) {
    //     println!("Reading messages of new connetions");

    //     let peer_addr = stream.peer_addr().unwrap();
    //     let buf_reader = BufReader::new(&*stream);
    //     let mut lines_iteration = buf_reader.lines();
    //     loop {
    //         match lines_iteration.next() {
    //             Some(Ok(message)) => {
    //                 println!("This is the message {message:?}");
    //                 broadcast_message(message, peer_addr, Arc::clone(&streams));
    //             },
    //             None => {
    //                 println!("Peer DISCONNECTED {peer_addr:?}");
    //                 let mut active_stream = streams.lock().unwrap();
    //                 active_stream.retain(|stream| {
    //                     stream.peer_addr().unwrap() != peer_addr
    //                 });
    //                 break;
    //             },
    //             Some(Err(e)) => {
    //                 println!("Got an error from a peer {peer_addr:?} Error: {e:?}");
    //                 break;
    //             }
    //         }
    //     }

    //     // while let Some(Ok(message)) = lines_iteration.next() {
    //     //     println!("This is the message {message:?}");
    //     //     broadcast_message(message, peer_addr, Arc::clone(&streams));
    //     // }
    // }

    // fn broadcast_message(message: String, peer_addr: SocketAddr, streams: Arc<Mutex<Vec<Arc<TcpStream>>>>) {
    //     println!("Broadcasting message");
    //     let streams_locked = streams.lock().expect("Streams N/A in broadcast_message");
    //     for  stream in  streams_locked.iter(){

    //         if peer_addr != stream.peer_addr().expect("Don't have the peer addr") {
    //             let mut raw_stream = &**stream;
    //             raw_stream.write_all(message.as_bytes()).unwrap();
    //         }
    //     }
    // }