fn main() {
    println!("Time for some practice!");

    let mut vec = vec![1, 2, 3, 4];
    println!("powerset of {:?}?", vec);
    let result = powerset(&mut vec);
    println!("{:?}", result);
}

fn powerset(list: &mut Vec<i32>) -> Vec<Vec<i32>> {
    let mut sets: Vec<Vec<i32>> = Vec::new();
    if list.len() == 0 {
        sets.push(list.to_vec());
    } else {
        let (head, tail) = list.split_at(1);
        for e in powerset(&mut tail.to_vec()) {
            sets.push(e.clone());
            sets.push([head, e.as_slice()].concat());
        }
    }
    return sets;
}
