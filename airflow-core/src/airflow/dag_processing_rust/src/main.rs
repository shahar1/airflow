// TODO: Signal handling
fn main() {


    let mut poll_time: u64 = 0.0 as u64;

    let loop_start_time = std::time::Instant::now();
    println!("Hello, world!");
    // poll_time = 1.0 as u64;
    let loop_duration = loop_start_time.elapsed().as_micros();
    // if (loop_duration < 1) {
    //     poll_time = 1 - loop_duration;
    // }
    // else {
    //     poll_time = 0.0 as u64;
    // }
    println!("Loop duration: {:.10}", loop_duration);
    // std::thread::sleep(std::time::Duration::from_secs(poll_time));

}
