use core::fmt;
use im::HashMap as ImMap;
use malachite::Integer;
use malachite::base::num::arithmetic::traits::{AddMul, FloorRoot, Mod, Pow};
use std::char;
use std::collections::HashMap;
use std::io::{self, Bytes, Read, Write, stdin, stdout};
use std::rc::Rc as Arc;

const ZERO: Integer = Integer::const_from_unsigned(0);
const ONE: Integer = Integer::const_from_unsigned(1);
const TEN: Integer = Integer::const_from_unsigned(10);

#[derive(Debug)]
enum Error {
    EndProgram,
    StackOutOfBounds,
    DivideByZero,
    BreakOutsideBlock,
    ReturnOutsideFunction,
    FunctionNotFound,
    IntTooBig,
    EofIntInput,
    BadIntInput,
    Io(io::Error),
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<Error> for Flow {
    fn from(e: Error) -> Self {
        Self::Error(e)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::EndProgram => write!(f, "program terminated"),
            Self::StackOutOfBounds => write!(f, "invalid stack access"),
            Self::DivideByZero => write!(f, "divide by zero"),
            Self::BreakOutsideBlock => write!(f, "break not inside a loop"),
            Self::ReturnOutsideFunction => write!(f, "returned not inside a function"),
            Self::FunctionNotFound => write!(f, "unknown function name"),
            Self::IntTooBig => write!(f, "integer too big"),
            Self::EofIntInput => write!(f, "unexpected end of file while expecting int input"),
            Self::BadIntInput => write!(f, "expected int input"),
            Self::Io(e) => write!(f, "io error: {e}"),
        }
    }
}

#[derive(Debug)]
enum Flow {
    Error(Error),
    Break,
    Return,
}

type R<T> = Result<T, Flow>;

#[derive(Clone, Debug)]
enum Inst {
    Load,
    Store,
    Pop,
    PopN,
    RotN,
    Dup,
    Pick,
    Swap,
    SwapN,
    Reverse,
    ReverseN,
    RotInverseN,
    Push(Integer),
    InputInt,
    InputChar,
    OutputInt,
    OutputChar,
    Break,
    Return,
    EndProgram,
    Call(Integer),
    If {
        then_body: Vec<Self>,
        else_body: Vec<Self>,
    },
    While {
        body: Vec<Self>,
        do_while: bool,
    },
    Function {
        name: Arc<Integer>,
        body: Arc<[Self]>,
    },
    Not,
    LessThan,
    Pow,
    Plus,
    Minus,
    Modulo,
    Increment,
    Decrement,
    Times,
    Divide,
}

fn parse(src: &str) -> Vec<Inst> {
    let mut digits = vec![];
    {
        let mut push_digit = |count| {
            if count == 0 {
                return;
            }
            if count > 10 {
                // yes this is stupid
                let digit_string = format!("{count}");
                for d in digit_string.chars() {
                    digits.push(d.to_digit(10).unwrap() as u8);
                }
            } else if count == 10 {
                digits.push(0);
            } else {
                digits.push(count as u8);
            }
        };
        let mut count: usize = 0;
        for c in src.chars() {
            if c.is_alphabetic() {
                count += 1;
            } else if c == '\'' || c == '\u{2018}' || c == '\u{2019}' {
                // pass
            } else {
                push_digit(count);
                count = 0;
            }
        }
        push_digit(count);
    }
    fn parse_multidigit(ds: &mut std::slice::Iter<u8>) -> Integer {
        let mut number = ZERO;
        loop {
            let length = *ds.next().expect("multi-digit number missing length");
            for _ in 0..length {
                let number_digit =
                    Integer::from(*ds.next().expect("multi-digit number unexpected end"));
                number = number_digit.add_mul(number, TEN);
            }
            if ds.as_slice().first() == Some(&9) {
                _ = ds.next().unwrap();
            } else {
                break;
            }
        }
        number
    }
    fn parse_block(ds: &mut std::slice::Iter<u8>) -> Vec<Inst> {
        let mut body = vec![];
        go(&mut body, ds);
        let digits = ds.as_slice();
        let Some(rest) = digits.strip_prefix(&[7, 3]) else {
            panic!("expected end of block");
        };
        *ds = rest.iter();
        body
    }
    fn go(instructions: &mut Vec<Inst>, ds: &mut std::slice::Iter<u8>) {
        let mut save_ds;
        // let mut meow = ds.as_slice();
        while let Some(&instruction) = {
            save_ds = ds.clone();
            ds.next()
        } {
            match instruction {
                // heap storage/retrieval
                1 => {
                    let subcommand = ds.next().expect("instruction 1 missing subcommand");
                    if subcommand % 2 != 0 {
                        instructions.push(Inst::Load);
                    } else {
                        instructions.push(Inst::Store);
                    }
                }
                // stack manipulation
                2 => {
                    let subcommand = ds.next().expect("instruction 2 missing subcommand");
                    let inst = match subcommand {
                        1 => Inst::Pop,
                        2 => Inst::PopN,
                        3 => Inst::RotN,
                        4 => Inst::Dup,
                        5 => Inst::Pick,
                        6 => Inst::Swap,
                        7 => Inst::SwapN,
                        8 => Inst::Reverse,
                        9 => Inst::ReverseN,
                        0 => Inst::RotInverseN,
                        10.. => unreachable!(),
                    };
                    instructions.push(inst);
                }
                // push multi-digit number
                3 => {
                    instructions.push(Inst::Push(parse_multidigit(ds)));
                }
                // push single digit number
                4 => {
                    let digit = *ds.next().expect("instruction 4 missing operand");
                    instructions.push(Inst::Push(digit.into()));
                }
                5 => {
                    let subcommand = ds.next().expect("instruction 5 missing subcommand");
                    if subcommand % 2 != 0 {
                        instructions.push(Inst::InputInt);
                    } else {
                        instructions.push(Inst::InputChar);
                    }
                }
                6 => {
                    let subcommand = ds.next().expect("instruction 6 missing subcommand");
                    if subcommand % 2 != 0 {
                        instructions.push(Inst::OutputInt);
                    } else {
                        instructions.push(Inst::OutputChar);
                    }
                }
                // control flow
                7 => {
                    let subcommand = ds.next().expect("instruction 7 missing subcommand");
                    let inst = match subcommand {
                        1 => Inst::Break,
                        2 => {
                            let mut then_body = vec![];
                            go(&mut then_body, ds);
                            let digits = ds.as_slice();
                            let else_body = if let Some(rest) = digits.strip_prefix(&[7, 3]) {
                                *ds = rest.iter();
                                vec![]
                            } else if let Some(rest) = digits.strip_prefix(&[7, 9]) {
                                *ds = rest.iter();
                                let mut body = vec![];
                                go(&mut body, ds);
                                body
                            } else {
                                panic!("expected end of if block");
                            };
                            Inst::If {
                                then_body,
                                else_body,
                            }
                        }
                        3 | 9 => {
                            *ds = save_ds;
                            return;
                        }
                        4 => Inst::Call(parse_multidigit(ds)),
                        5 => {
                            let body = parse_block(ds);
                            Inst::While {
                                body,
                                do_while: false,
                            }
                        }
                        6 => Inst::Return,
                        7 => {
                            let body = parse_block(ds);
                            Inst::While {
                                body,
                                do_while: true,
                            }
                        }
                        8 => {
                            let name = Arc::new(parse_multidigit(ds));
                            let body = parse_block(ds).into();
                            Inst::Function { name, body }
                        }
                        0 => Inst::EndProgram,
                        10.. => unreachable!(),
                    };
                    instructions.push(inst);
                }
                // arithmetic
                8 => {
                    let subcommand = ds.next().expect("instruction 8 missing subcommand");
                    let inst = match subcommand {
                        1 => Inst::Not,
                        2 => Inst::LessThan,
                        3 => Inst::Pow,
                        4 => Inst::Plus,
                        5 => Inst::Minus,
                        6 => Inst::Modulo,
                        7 => Inst::Increment,
                        8 => Inst::Decrement,
                        9 => Inst::Times,
                        0 => Inst::Divide,
                        10.. => unreachable!(),
                    };
                    instructions.push(inst);
                }
                9 => panic!("unexpected 9"),
                0 => {
                    let i = ds
                        .position(|&d| d == 0)
                        .expect("expected end of block comment definition");
                    let comment_chunk = &save_ds.as_slice()[..i + 2];
                    //eprintln!("{comment_chunk:?}");
                    assert_eq!(comment_chunk.first(), Some(&0));
                    assert_eq!(comment_chunk.last(), Some(&0));
                    loop {
                        if let Some(rest) = ds.as_slice().strip_prefix(comment_chunk) {
                            *ds = rest.iter();
                            break;
                        }
                        ds.next().expect("expected end of block comment");
                    }
                }
                10.. => unreachable!(),
            }
        }
    }
    let mut instructions = vec![];
    let mut ds = digits.iter();
    go(&mut instructions, &mut ds);
    eprintln!("{instructions:?}");
    let ds = ds.as_slice();
    if !ds.is_empty() {
        panic!("more program {ds:?}");
    }
    instructions
}

#[derive(Debug)]
struct Env<Reading, Writing> {
    stack: Vec<Integer>,
    heap: HashMap<Integer, Integer>,
    functions: ImMap<Arc<Integer>, Arc<[Inst]>>,
    input: Bytes<Reading>,
    input_peek: Option<u8>,
    output: Writing,
}

impl<Reading: Read, Writing: Write> Env<Reading, Writing> {
    const UNDERFLOW: Flow = Flow::Error(Error::StackOutOfBounds);
    pub fn new(input: Reading, output: Writing) -> Self {
        Self {
            stack: vec![],
            heap: HashMap::new(),
            functions: ImMap::new(),
            input: input.bytes(),
            input_peek: None,
            output,
        }
    }
    fn push(&mut self, int: Integer) {
        self.stack.push(int);
    }
    fn peek(&self) -> R<Integer> {
        self.stack.last().cloned().ok_or(Self::UNDERFLOW)
    }
    fn pop(&mut self) -> R<Integer> {
        self.stack.pop().ok_or(Self::UNDERFLOW)
    }
    fn to_nth(&self, i: Integer) -> R<(usize, bool)> {
        use std::cmp::Ordering::*;
        // yes, this is awful!
        match i.cmp(&ZERO) {
            Less => {
                let i = usize::try_from(&(-i - ONE)).map_err(|_| Self::UNDERFLOW)?;
                if i >= self.stack.len() {
                    return Err(Self::UNDERFLOW);
                }
                Ok((i, true))
            }
            Equal => panic!("0 index"),
            Greater => {
                let i: usize = usize::try_from(&i).map_err(|_| Self::UNDERFLOW)?;
                let i = self.stack.len().checked_sub(i - 1).ok_or(Self::UNDERFLOW)?;
                Ok((i, false))
            }
        }
    }
    fn pop_nth(&mut self) -> R<usize> {
        let i = self.pop()?;
        self.to_nth(i).map(|(nth, _)| nth)
    }
    fn get_byte(&mut self) -> R<Option<u8>> {
        if let Some(byte) = self.input_peek.take() {
            Ok(Some(byte))
        } else {
            match self.input.next().transpose().map_err(Error::Io)? {
                Some(byte) => Ok(Some(byte)),
                None => return Ok(None),
            }
        }
    }
    fn get_char(&mut self) -> R<Option<char>> {
        let Some(c) = self.get_byte()? else {
            return Ok(None);
        };
        if c < 128 {
            return Ok(Some(c as char));
        }
        let mut buffer = vec![c];
        loop {
            let Some(c) = self.input.next().transpose().map_err(Error::Io)? else {
                // end of input
                break;
            };
            if c < 128 {
                self.input_peek = Some(c);
                break;
            }
            buffer.push(c);
        }
        match String::from_utf8(buffer) {
            Ok(string) => Ok(Some(string.chars().next().unwrap())),
            Err(_) => Ok(Some(char::REPLACEMENT_CHARACTER)),
        }
    }
    pub fn run_insts(&mut self, insts: &[Inst]) -> R<()> {
        if insts.is_empty() {
            return Ok(());
        }
        let current_scope = self.functions.clone();
        for inst in insts {
            self.run_inst(inst)?;
        }
        self.functions = current_scope;
        Ok(())
    }
    pub fn run_inst(&mut self, inst: &Inst) -> R<()> {
        use Inst::*;
        //eprint!("{inst:?} ");
        stdout().flush().unwrap();
        match inst {
            Load => {
                let addr = self.pop()?;
                let value = self.heap.get(&addr).cloned().unwrap_or_default();
                self.push(value);
            }
            Store => {
                let addr = self.pop()?;
                let value = self.pop()?;
                self.heap.insert(addr, value);
            }
            Pop => {
                _ = self.pop()?;
            }
            PopN => {
                let nth = self.pop_nth()?;
                _ = self.stack.remove(nth);
            }
            RotN => {
                let index = self.pop()?;
                let top = self.pop()?;
                let nth = self.to_nth(index)?.0;
                self.stack.insert(nth, top);
            }
            Dup => {
                let int = self.peek()?;
                self.push(int);
            }
            Pick => {
                let nth = self.pop_nth()?;
                self.push(self.stack[nth].clone());
            }
            Swap => {
                let a = self.pop()?;
                let b = self.pop()?;
                self.push(a);
                self.push(b);
            }
            SwapN => {
                let nth = self.pop_nth()?;
                let len = self.stack.len();
                if let Ok([a, b]) = self.stack.get_disjoint_mut([nth, len - 1]) {
                    std::mem::swap(a, b);
                }
            }
            Reverse => self.stack.reverse(),
            ReverseN => {
                let index = self.pop()?;
                let (nth, is_neg) = self.to_nth(index)?;
                if is_neg {
                    self.stack[..nth].reverse();
                } else {
                    self.stack[nth..].reverse();
                }
            }
            RotInverseN => {
                let nth = self.pop_nth()?;
                let new_top = self.stack.remove(nth);
                self.push(new_top);
            }
            Push(int) => self.push(int.clone()),
            InputInt => {
                let mut c;
                loop {
                    c = self.get_char()?.ok_or(Error::EofIntInput)?;
                    if !c.is_whitespace() {
                        break;
                    }
                }
                let is_negative = if c == '-' {
                    c = self.get_char()?.ok_or(Error::EofIntInput)?;
                    true
                } else {
                    false
                };
                if !c.is_ascii_digit() {
                    Err(Error::BadIntInput)?;
                }
                let mut n = Integer::from(c as u8 - b'0');
                loop {
                    match self.get_byte()? {
                        Some(digit) if digit.is_ascii_digit() => {
                            n = Integer::from(digit - b'0').add_mul(n, TEN);
                        }
                        Some(c) => {
                            // i think this assumption holds given how we parse bytes and how digits are ascii
                            assert_eq!(self.input_peek, None);
                            self.input_peek = Some(c);
                            break;
                        }
                        None => break,
                    }
                }
                if is_negative {
                    n = -n;
                }
                self.push(n);
            }
            InputChar => {
                let codepoint = match self.get_char()? {
                    Some(c) => u32::from(c).into(),
                    None => Integer::from(-1),
                };
                self.push(codepoint);
            }
            OutputInt => {
                let int = self.pop()?;
                write!(self.output, "{int}").map_err(Error::Io)?;
            }
            OutputChar => {
                let char = u32::try_from(&self.pop()?)
                    .ok()
                    .and_then(char::from_u32)
                    .unwrap_or(char::REPLACEMENT_CHARACTER);
                write!(self.output, "{char}").map_err(|e| Flow::Error(Error::Io(e)))?;
            }
            Break => return Err(Flow::Break),
            If {
                then_body,
                else_body,
            } => {
                let cond = self.pop()?;
                let run_body = if cond != ZERO { then_body } else { else_body };
                self.run_insts(&run_body)?;
            }
            Call(name) => {
                let body = self
                    .functions
                    .get(name)
                    .ok_or(Flow::Error(Error::FunctionNotFound))?
                    .clone();
                match self.run_insts(&body) {
                    Err(Flow::Return) => (),
                    Err(Flow::Break) => return Err(Flow::Error(Error::BreakOutsideBlock)),
                    result => result?,
                }
            }
            While { body, do_while } => {
                let mut check_cond = !do_while;
                loop {
                    if check_cond && self.pop()? == ZERO {
                        break;
                    }
                    check_cond = true;
                    match self.run_insts(body) {
                        Err(Flow::Break) => (),
                        result => result?,
                    }
                }
            }
            Return => return Err(Flow::Return),
            Function { name, body } => {
                self.functions.insert(name.clone(), body.clone());
            }
            EndProgram => return Err(Flow::Error(Error::EndProgram)),
            Not => {
                let x = self.pop()?;
                self.push(Integer::from(x == ZERO));
            }
            LessThan => {
                let cond = self.pop()? < self.pop()?;
                self.push(cond.into());
            }
            Pow => {
                let base = self.pop()?;
                let exp = self.pop()?;
                let power = if base == ONE {
                    ONE
                } else {
                    use std::cmp::Ordering::*;
                    match exp.cmp(&ZERO) {
                        // negative, interpreted in Bespoke as exp-th root of base
                        Less => {
                            if let Ok(exp) = u64::try_from(&-exp) {
                                base.floor_root(exp)
                            } else {
                                // is this correct? i suppose it is so long as all our numbers
                                // are less than 2 ** 2 ** 64, which seems relatively safe
                                ONE
                            }
                        }
                        Equal => ONE,
                        // exponentiation by squaring algorithm
                        Greater => {
                            let exp = u64::try_from(&exp).map_err(|_| Error::IntTooBig)?;
                            base.pow(exp)
                        }
                    }
                };
                self.push(power);
            }
            Plus => {
                let sum = self.pop()? + self.pop()?;
                self.push(sum);
            }
            Minus => {
                let difference = self.pop()? - self.pop()?;
                self.push(difference);
            }
            Modulo => {
                let x = self.pop()?;
                let y = self.pop()?;
                if y == ZERO {
                    Err(Error::DivideByZero)?;
                }
                self.push(x.mod_op(y));
            }
            Increment => {
                let sum = self.pop()? + ONE;
                self.push(sum);
            }
            Decrement => {
                let sum = self.pop()? - ONE;
                self.push(sum);
            }
            Times => {
                let product = self.pop()? * self.pop()?;
                self.push(product);
            }
            Divide => {
                let x = self.pop()?;
                let y = self.pop()?;
                if y == ZERO {
                    Err(Error::DivideByZero)?;
                }
                self.push(x / y);
            } //_ => todo!(),
        }
        //eprintln!("{:?}", self.stack);
        Ok(())
    }
}

fn run<R: Read, W: Write>(src: &str, r: R, w: W) -> Result<(), Error> {
    let insts = parse(src);
    let mut env = Env::new(r, w);
    match env.run_insts(&insts) {
        Ok(()) | Err(Flow::Error(Error::EndProgram)) => Ok(()),
        Err(Flow::Error(e)) => Err(e),
        Err(Flow::Break) => Err(Error::BreakOutsideBlock),
        Err(Flow::Return) => Err(Error::ReturnOutsideFunction),
    }
}

fn main() {
    let _hello_world0 = "more peppermint tea?
ah yes, it's not bad
I appreciate peppermint tea
it's a refreshing beverage

but you immediately must try the gingerbread
I had it sometime, forever ago
oh, and it was so good!
made the way a gingerbread must clearly be 
baked

in fact, I've got a suggestion
I may go outside
to Marshal Mellow's Bakery
so we both receive one";
    let _truth_machine = "there I stumble
falling up
even whilst I go down
falling out";
    let fibonacci = r#"read a note I wrote you
it says "someone shall love you,
as big as past lovers did,
put in a collection"
surely it will be
in truth, somebody here is probably isolated
existing in pain without you"#;
    let mut buffer: Vec<u8> = vec![];
    let result = run(fibonacci, stdin(), &mut buffer);
    if let Err(e) = result {
        eprintln!("{e}");
    }
    print!("{}", String::from_utf8(buffer).unwrap())
}
