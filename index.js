const faker = require('faker')
const uuid = require('uuid')
const Bluebird = require('bluebird')
const { range, map, find, differenceBy, sumBy, filter } = require('lodash')

let done = false

async function run() {
  const loadId = 22
  const part1 = map(range(10), generate(loadId))
  const part2 = map(range(10), generate(loadId))
  const part3 = map(range(10), generate(loadId))
  const part4 = map(range(10), generate(loadId))
  consumeStart({
    id: loadId,
    leftCount: part1.length + part2.length + part3.length,
    rightCount: part1.length + part2.length + part4.length
  })
  consumeLeft(part1, 0)
  consumeLeft(part2, 0)
  consumeLeft(part3, 0)
  consumeRight(part1, 0)
  consumeRight(part2, 0)
  consumeRight(part4, 0)
  while (!done) {
    await Bluebird.delay(100)
    console.log(db.joined.length)
  }
  console.log(JSON.stringify(db.joined, null, 2))
  console.log(db.joined.length)
}

function consumeLeft(part, offset) {
  if (offset >= part.length) return
  const message = part[offset]
  db.insertLeft({
    name: message.name,
    id: message.id,
    loadId: message.loadId
  })
  setTimeout(consumeLeft, Math.random() * 10, part, offset + 1)
}

function consumeRight(part, offset) {
  if (offset >= part.length) return
  const message = part[offset]
  db.insertRight({
    email: message.email,
    id: message.id,
    loadId: message.loadId
  })
  setTimeout(consumeRight, Math.random() * 10, part, offset + 1)
}

function consumeStart(message) {
  db.insertLoad(message)
}

const db = {
  left: [],
  right: [],
  joined: [],
  loads: [],
  insertLeft(message) {
    const { loadId } = message
    this.left.push(message)
    const right = find(
      this.right,
      r => r.id === message.id && r.loadId === loadId
    )
    if (right) {
      this.joined.push({ ...message, ...right })
    }
    const { leftCount, rightCount } = find(this.loads, l => l.id === loadId)
    if (
      leftCount === sumBy(this.left, l => l.loadId === loadId) &&
      rightCount === sumBy(this.right, r => r.loadId === loadId)
    )
      this.finish(loadId)
  },
  insertRight(message) {
    const { loadId } = message
    this.right.push(message)
    const left = find(
      this.left,
      l => l.id === message.id && l.loadId === loadId
    )
    if (left) {
      this.joined.push({ ...message, ...left })
    }
    const { leftCount, rightCount } = find(this.loads, l => l.id === loadId)
    if (
      leftCount === sumBy(this.left, l => l.loadId === loadId) &&
      rightCount === sumBy(this.right, r => r.loadId === loadId)
    )
      this.finish(loadId)
  },
  insertLoad(details) {
    this.loads.push(details)
  },
  finish(loadId) {
    const left = filter(this.left, l => l.loadId === loadId)
    const right = filter(this.right, r => r.loadId === loadId)
    const remainingLeft = differenceBy(left, this.joined, i => i.id)
    const remainingRight = differenceBy(right, this.joined, i => i.id)
    this.joined = this.joined.concat(remainingLeft).concat(remainingRight)
    done = true
  }
}

const generate = loadId => () => {
  const name = faker.name.findName()
  const [firstName, lastName] = name.split(' ')
  return {
    loadId,
    id: uuid(),
    name,
    email: faker.internet.email(firstName, lastName)
  }
}

run()
  .catch(ex => {
    console.error(ex)
    process.exit(1)
  })
  .then(() => {
    process.exit(0)
  })
