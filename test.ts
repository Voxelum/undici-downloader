async function* test() {
    while (true) {
        yield 1
    }
}

async function main() {
    const generator = test()
    const v = await generator.next()
    console.log(v)
}



