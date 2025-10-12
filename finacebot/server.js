require('dotenv').config();
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const mysql = require('mysql2/promise');
const app = express();
const server = http.createServer(app);
const io = new Server(server);

app.use(express.static('public'));

io.on('connection', (socket) => {
  console.log('dashboard conectado');
});

async function pollOffersAndEmit() {
  const conn = await mysql.createConnection({host:process.env.MYSQL_HOST||'127.0.0.1', user:process.env.MYSQL_USER||'root', password:process.env.MYSQL_PASS||'', database:process.env.MYSQL_DB||'promo_bot'});
  while(true){
    try{
      const [rows] = await conn.execute('SELECT id,title,price,url,image_url,fetched_at FROM offers ORDER BY fetched_at DESC LIMIT 20');
      io.emit('offers_update', rows);
    }catch(e){}
    await new Promise(r=>setTimeout(r,10000));
  }
}

server.listen(3000, () => {
  console.log('server ouvindo na porta 3000');
  pollOffersAndEmit();
});
