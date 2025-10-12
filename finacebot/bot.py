import asyncio
import os
import hmac
import hashlib
import json
from datetime import date
from dotenv import load_dotenv
from aiohttp import ClientSession
import aiomysql
from telegram import Bot
from telegram.ext import ApplicationBuilder, ContextTypes, CommandHandler, MessageHandler, filters

load_dotenv()
TELEGRAM_TOKEN=os.getenv("TELEGRAM_TOKEN")
MYSQL_HOST=os.getenv("MYSQL_HOST","127.0.0.1")
MYSQL_PORT=int(os.getenv("MYSQL_PORT","3306"))
MYSQL_USER=os.getenv("MYSQL_USER","root")
MYSQL_PASS=os.getenv("MYSQL_PASS","")
MYSQL_DB=os.getenv("MYSQL_DB","promo_bot")
SHOPPE_APP_ID=os.getenv("SHOPPE_APP_ID","")
SHOPPE_APP_SECRET=os.getenv("SHOPPE_APP_SECRET","")

async def get_pool():
    return await aiomysql.create_pool(host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER, password=MYSQL_PASS, db=MYSQL_DB, autocommit=True)

async def init_db_pool(app):
    app["pool"]=await get_pool()

async def close_db_pool(app):
    pool=app.get("pool")
    if pool:
        pool.close()
        await pool.wait_closed()

async def ensure_user(pool, telegram_user):
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT id FROM users WHERE telegram_id=%s", (telegram_user.id,))
            r=await cur.fetchone()
            if r:
                return r[0]
            await cur.execute("INSERT INTO users (telegram_id, username, first_name, last_name) VALUES (%s,%s,%s,%s)", (telegram_user.id, telegram_user.username or "", telegram_user.first_name or "", telegram_user.last_name or ""))
            return cur.lastrowid

async def insert_offer(pool, data):
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("INSERT INTO offers (external_id,title,store,price,old_price,url,image_url,category) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE price=VALUES(price),old_price=VALUES(old_price),fetched_at=CURRENT_TIMESTAMP", (data.get("external_id"), data.get("title"), data.get("store"), data.get("price"), data.get("old_price"), data.get("url"), data.get("image_url"), data.get("category")))
            await cur.execute("SELECT id FROM offers WHERE external_id=%s AND store=%s", (data.get("external_id"), data.get("store")))
            r=await cur.fetchone()
            return r[0] if r else None

async def fetch_shopee_offers(session, query="notebook", limit=10):
    url="https://partner.shopeemobile.com/api/v1/items/search"
    headers={"Content-Type":"application/json"}
    payload={"query":query,"limit":limit}
    async with session.post(url,json=payload,headers=headers) as r:
        if r.status==200:
            return await r.json()
        return None

async def prepare_and_store_offers(pool, raw):
    stored=[]
    if not raw:
        return stored
    items = raw.get("items") or raw.get("data") or raw.get("result") or []
    for it in items:
        data={
            "external_id": str(it.get("itemid") or it.get("id") or it.get("external_id") or ""),
            "title": it.get("name") or it.get("title") or "",
            "store": it.get("shop_name") or it.get("store") or "shopee",
            "price": float((it.get("price") or it.get("current_price") or 0))/100 if isinstance(it.get("price"), int) else float(it.get("price") or 0),
            "old_price": float(it.get("old_price") or 0),
            "url": it.get("url") or "",
            "image_url": it.get("image") or "",
            "category": it.get("category") or ""
        }
        oid=await insert_offer(pool,data)
        if oid:
            stored.append((oid,data))
    return stored

async def broadcast_offers_to_users(app, offers_stored):
    pool=app["pool"]
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT id,telegram_id FROM users")
            users=await cur.fetchall()
    bot=Bot(TELEGRAM_TOKEN)
    for oid,data in offers_stored:
        text=f"{data['title']}\nPreço: R${data['price']:.2f}"
        if data["old_price"] and data["old_price"]>data["price"]:
            text+=f" (De R${data['old_price']:.2f})"
        if data["url"]:
            text+=f"\n{data['url']}"
        for uid,tgid in users:
            try:
                await bot.send_message(chat_id=tgid, text=text)
                async with pool.acquire() as conn:
                    async with conn.cursor() as cur:
                        await cur.execute("INSERT INTO sent_offers (user_id,offer_id,delivered) VALUES (%s,%s,%s)", (uid, oid, 1))
            except Exception:
                async with pool.acquire() as conn:
                    async with conn.cursor() as cur:
                        await cur.execute("INSERT INTO sent_offers (user_id,offer_id,delivered) VALUES (%s,%s,%s)", (uid, oid, 0))

async def worker_fetch_and_send(app):
    pool=app["pool"]
    async with ClientSession() as session:
        raw=await fetch_shopee_offers(session, query="smartphone", limit=5)
        stored=await prepare_and_store_offers(pool, raw)
        if stored:
            await broadcast_offers_to_users(app, stored)

async def cmd_start(update, context: ContextTypes.DEFAULT_TYPE):
    pool=context.application["pool"]
    uid=await ensure_user(pool, update.effective_user)
    await context.bot.send_message(chat_id=update.effective_chat.id, text="Bem vindo! Você será registrado para receber ofertas.")
    return

async def cmd_add_purchase(update, context: ContextTypes.DEFAULT_TYPE):
    pool=context.application["pool"]
    parts=context.args
    if len(parts)<1:
        await context.bot.send_message(chat_id=update.effective_chat.id, text="Use: /compra <valor> [descrição]")
        return
    try:
        amount=float(parts[0].replace(",",".")) 
    except:
        await context.bot.send_message(chat_id=update.effective_chat.id, text="Valor inválido")
        return
    desc=" ".join(parts[1:])[:500]
    uid=await ensure_user(pool, update.effective_user)
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("INSERT INTO purchases (user_id,amount,description,purchase_date) VALUES (%s,%s,%s,%s)", (uid, amount, desc, date.today()))
    await context.bot.send_message(chat_id=update.effective_chat.id, text=f"Compra registrada: R${amount:.2f}")

async def cmd_report(update, context: ContextTypes.DEFAULT_TYPE):
    pool=context.application["pool"]
    uid=await ensure_user(pool, update.effective_user)
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT SUM(amount) FROM purchases WHERE user_id=%s", (uid,))
            r=await cur.fetchone()
            total=r[0] or 0
    await context.bot.send_message(chat_id=update.effective_chat.id, text=f"Total gasto: R${float(total):.2f}")

async def periodic_task(application):
    while True:
        try:
            await worker_fetch_and_send(application)
        except Exception:
            pass
        await asyncio.sleep(60*10)

def main():
    app=ApplicationBuilder().token(TELEGRAM_TOKEN).build()
    app.post_init.append(init_db_pool)
    app.post_shutdown.append(close_db_pool)
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("compra", cmd_add_purchase))
    app.add_handler(CommandHandler("relatorio", cmd_report))
    loop=asyncio.get_event_loop()
    loop.create_task(periodic_task(app))
    app.run_polling()

if __name__=="__main__":
    main()
