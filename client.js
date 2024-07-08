const mqtt = require('mqtt')
const axios = require('axios')
const fs = require('fs')
const pool = require('./db')

const brokerUrl = 'mqtt://broker.hivemq.com'
const topic = 'test/topic'
const client = mqtt.connect(brokerUrl)

const pageSize = 10
const statusFile = 'status.json'

function saveCurrentPage(page) {
    fs.writeFileSync(statusFile, JSON.stringify({ currentPage: page }))
}

function loadCurrentPage() {
    if (fs.existsSync(statusFile)) {
        const data = fs.readFileSync(statusFile)
        return JSON.parse(data).currentPage
    }
    return 1
}

let currentPage = loadCurrentPage()

async function fetchData(page, limit) {
    try {
        const response = await axios.get(`https://jsonplaceholder.typicode.com/posts`, {
            params: {
                _page: page,
                _limit: limit
            }
        })
        console.log(`Fetched data for page ${page}:`, response.data)
        return response.data
    } catch (error) {
        console.error('Error fetching data from API:', error)
        return null
    }
}

async function saveDataToDatabase(data) {
    const client = await pool.connect()
    try {
        const query = 'INSERT INTO app_public.posts (id, title, body, user_id) VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING'
        for (const item of data) {
            await client.query(query, [item.id, item.title, item.body, item.userId])
        }
        console.log('Data successfully saved to database.')
    } catch (error) {
        console.error('Error saving data to database:', error)
    } finally {
        client.release()
    }
}

async function isDataInDatabase(id) {
    const client = await pool.connect()
    try {
        const query = 'SELECT COUNT(*) FROM app_public.posts WHERE id = $1'
        const result = await client.query(query, [id])
        return result.rows[0].count > 0
    } catch (error) {
        console.error('Error checking data in database:', error)
        return false
    } finally {
        client.release()
    }
}

client.on('connect', () => {
    console.log('Connected to broker')
    client.subscribe(topic, (err) => {
        if (!err) {
            console.log(`Subscribed to topic: ${topic}`)
            sendDataToMqtt()
        } else {
            console.error('Failed to subscribe:', err)
        }
    })
})

client.on('message', (topic, message) => {
    console.log(`Received message on topic ${topic}: ${message.toString()}`)
})

client.on('error', (err) => {
    console.error('Connection error:', err)
})

client.on('close', () => {
    console.log('Connection closed')
})

async function sendDataToMqtt() {
    let data = await fetchData(currentPage, pageSize)
    if (data && data.length > 0) {
        const newData = []
        for (const item of data) {
            const exists = await isDataInDatabase(item.id)
            if (!exists) {
                newData.push(item)
            }
        }
        if (newData.length > 0) {
            await saveDataToDatabase(newData)
            client.publish(topic, JSON.stringify(newData))
        } else {
            console.log('No new data to save.')
        }
    } else {
        console.log('No more data available.')
    }

    currentPage++
    if (data && data.length === 0) {
        currentPage = 1
    }
    setTimeout(sendDataToMqtt, 30000)
    saveCurrentPage(currentPage)
}