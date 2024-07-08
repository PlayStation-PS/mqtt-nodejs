const pool = require('./db')
const client = require('./client')

async function startApp() {
    try {
        await pool.connect()
        console.log('Connected to the database')

        console.log('Starting MQTT client...')
    } catch (error) {
        console.error('Error starting application', error)
    }
}

startApp()