import os
import logging
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, ContextTypes, filters
from openai import OpenAI
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

# --- Configuração de Logging ---
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# --- Carregar Variáveis de Ambiente ---
load_dotenv()
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
DISCOUNT_API_KEY = os.getenv("DISCOUNT_API_KEY")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# --- Configuração de Banco de Dados (SQLite para MVP, fácil migração) ---
# Em um deploy real no Render, você usaria um banco de dados externo (PostgreSQL)
# Mas para manter o código funcional e autocontido, usaremos SQLite.
DATABASE_URL = "sqlite:///financebot.db"
Engine = create_engine(DATABASE_URL)
Base = declarative_base()

# Definição do Modelo de Compra
class Purchase(Base):
    __tablename__ = 'purchases'
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer)
    product = Column(String)
    value = Column(Float)
    category = Column(String)
    date = Column(DateTime, default=datetime.now)

# Cria as tabelas no banco de dados (se não existirem)
Base.metadata.create_all(Engine)
Session = sessionmaker(bind=Engine)

# --- Configuração da OpenAI ---
if OPENAI_API_KEY:
    openai_client = OpenAI(api_key=OPENAI_API_KEY)
else:
    logger.warning("OPENAI_API_KEY não encontrado. A funcionalidade de IA estará desativada.")
    openai_client = None

# --- Funções de API e Scraping ---

async def buscar_discount_api_real(context: ContextTypes.DEFAULT_TYPE):
    """Busca promoções na DiscountAPI."""
    # ... (código da DiscountAPI permanece o mesmo) ...
    logger.info("🔍 Buscando promoções na DiscountAPI...")
    url = "https://api.discountapi.com/v2/deals"
    params = {
        "api_key": DISCOUNT_API_KEY,
        "limit": 5 # Limitar para 5 para uma resposta rápida
    }
    
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        deals = data.get("deals", [])
        
        if not deals:
            return "Nenhuma promoção incrível encontrada no momento. Tente mais tarde!"

        message_text = "✨ **Melhores Deals do Dia** ✨\n\n"
        
        for item in deals:
            deal = item.get("deal", {})
            title = deal.get("title", "Sem Título")
            price = deal.get("price", "N/A")
            discount = deal.get("discount_percentage", 0)
            provider = deal.get("provider", "Loja Desconhecida")
            link = deal.get("url", "#")
            
            # Formatação melhorada com links clicáveis
            message_text += (
                f"🏷️ **{title}**\n"
                f"💰 Preço: ${price} | Desconto: {discount:.1f}%\n"
                f"🏪 Loja: {provider}\n"
                f"[🔗 Ver Oferta]({link})\n\n"
            )
            logger.info(f"✅ {title} - ${price} ({discount:.1f}% OFF)")
            
        logger.info(f"✅ Total de {len(deals)} promoções encontradas!")
        return message_text
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Erro ao buscar DiscountAPI: {e}")
        return "❌ Ocorreu um erro ao buscar as promoções. Verifique a chave da API."



# --- Nova Função: IA para Análise Financeira e Respostas Inteligentes ---

async def buscar_mercado_livre(termo: str):
    """Busca produtos no Mercado Livre usando a API pública."""
    logger.info(f"🛒 Buscando no Mercado Livre por: {termo}")
    
    # API pública do Mercado Livre para busca no Brasil (MLB)
    url = "https://api.mercadolibre.com/sites/MLB/search"
    params = {
        "q": termo,
        "limit": 3 # Limitar a 3 resultados para concisão no Telegram
    }
    
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        results = data.get("results", [])
        
        if not results:
            return f"Nenhum produto encontrado no Mercado Livre para '{termo}'."

        message_text = f"🛒 **Resultados do Mercado Livre para '{termo}'** 🛒\n\n"
        
        for item in results:
            title = item.get("title", "Sem Título")
            price = item.get("price", "N/A")
            link = item.get("permalink", "#")
            
            message_text += (
                f"🏷️ **{title}**\n"
                f"💰 Preço: R$ {price:.2f}\n"
                f"[🔗 Ver Oferta]({link})\n\n"
            )
            
        return message_text
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Erro ao buscar Mercado Livre: {e}")
        return "❌ Ocorreu um erro ao buscar no Mercado Livre. Verifique a conexão."

async def analisar_com_ia(user_id: int, prompt: str):
    """Usa a IA para analisar o histórico de gastos e responder perguntas."""
    if not openai_client:
        return "❌ A funcionalidade de IA está desativada (chave da OpenAI não configurada)."

    session = Session()
    purchases = session.query(Purchase).filter_by(user_id=user_id).all()
    session.close()

    if not purchases:
        context_data = "O usuário ainda não registrou nenhuma compra."
    else:
        # Formata o histórico de compras para o prompt da IA
        history = "\n".join([
            f"- {p.product} (R$ {p.value:.2f}) na categoria {p.category} em {p.date.strftime('%d/%m/%Y')}"
            for p in purchases
        ])
        total_spent = sum(p.value for p in purchases)
        context_data = (
            f"O usuário já gastou um total de R$ {total_spent:.2f} em {len(purchases)} compras. "
            "Aqui está o histórico detalhado:\n"
            f"{history}"
        )

    system_prompt = (
        "Você é o FinanceBot, um assistente financeiro e de compras amigável. "
        "Sua função é analisar o histórico de gastos do usuário e responder a perguntas de forma útil e inteligente, **focando estritamente em gestão financeira, economia e análise de gastos**. "
        "Use os dados fornecidos para dar conselhos financeiros, identificar padrões de gastos ou responder a perguntas sobre orçamento. **Recuse-se educadamente a responder perguntas que não sejam sobre finanças ou o uso do bot.**"
        "Mantenha a resposta concisa e em português."
    )

    user_prompt = (
        f"Contexto de gastos do usuário:\n{context_data}\n\n"
        f"Pergunta do usuário: {prompt}"
    )

    try:
        response = openai_client.chat.completions.create(
            model="gpt-4.1-mini", # Usando um modelo eficiente
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ]
        )
        return response.choices[0].message.content
    except Exception as e:
        logger.error(f"Erro na chamada da OpenAI: {e}")
        return "❌ Ocorreu um erro ao consultar a Inteligência Artificial. Verifique sua chave ou limites de uso."

# --- Handlers de Comandos e Mensagens ---

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Envia a mensagem de boas-vindas e o menu principal."""
    keyboard = [
        [InlineKeyboardButton("💰 Melhores Deals (DiscountAPI)", callback_data='deals')],
        [InlineKeyboardButton("🛒 Buscar no Mercado Livre", callback_data='ml_search')],
        [InlineKeyboardButton("💳 Adicionar Compra", callback_data='add_purchase')],
        [InlineKeyboardButton("📊 Meus Gastos", callback_data='my_expenses')],
        [InlineKeyboardButton("🧠 Perguntar à IA", callback_data='ask_ai')], # Nova opção
        [InlineKeyboardButton("❓ Ajuda", callback_data='help')],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        'Olá! Eu sou o FinanceBot. Escolha uma opção abaixo para começar a economizar e rastrear seus gastos:',
        reply_markup=reply_markup
    )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Envia o guia de uso completo."""
    help_text = (
        "❓ **Guia de Uso do FinanceBot** ❓\n\n"
        "**💰 Melhores Deals:** Busca as melhores promoções internacionais (em USD).\n"
        "**🛒 Buscar no Mercado Livre:** Permite buscar produtos no Mercado Livre. Você será solicitado a digitar o termo de busca.\n"
        "**💳 Adicionar Compra:** Registra uma compra. Use o formato:\n"
        "   `Produto - Valor - Categoria` (Ex: `iPhone 15 - 5000 - Eletrônicos`)\n"
        "**📊 Meus Gastos:** Mostra o resumo das suas compras registradas.\n"
        "**🧠 Perguntar à IA:** Use a IA para analisar seus gastos e tirar dúvidas financeiras.\n"
    )
    await update.message.reply_text(help_text)

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Processa mensagens de texto para busca na Shopee, registro de compras ou IA."""
    user_id = update.effective_user.id
    text = update.message.text.strip()
    
    state = context.user_data.get('state')
    
    if state == 'waiting_ml_term':
        context.user_data['state'] = None
        result = await buscar_mercado_livre(text)
        await update.message.reply_text(result, parse_mode='Markdown')
        
    elif state == 'waiting_purchase':
        context.user_data['state'] = None
        
        # Validação do formato: Produto - Valor - Categoria
        parts = [p.strip() for p in text.split('-')]
        
        if len(parts) == 3:
            produto, valor_str, categoria = parts
            try:
                # Persistência de dados (salva no banco)
                valor = float(valor_str.replace(',', '.'))
                session = Session()
                new_purchase = Purchase(
                    user_id=user_id,
                    product=produto,
                    value=valor,
                    category=categoria
                )
                session.add(new_purchase)
                session.commit()
                session.close()
                
                await update.message.reply_text(
                    f"✅ Compra registrada com sucesso no banco de dados!\n"
                    f"Produto: {produto}\n"
                    f"Valor: R$ {valor:.2f}\n"
                    f"Categoria: {categoria}"
                )
                
            except ValueError:
                await update.message.reply_text("❌ Formato de valor inválido. Use apenas números (ex: 5000 ou 5000.50).")
        else:
            await update.message.reply_text(
                "❌ Formato incorreto. Use: `Produto - Valor - Categoria`\n"
                "Exemplo: `iPhone 15 - 5000 - Eletrônicos`"
            )
            
    elif state == 'waiting_ai_prompt':
        context.user_data['state'] = None
        await update.message.reply_text("🧠 Analisando sua pergunta com a IA...")
        result = await analisar_com_ia(user_id, text)
        await update.message.reply_text(result)
            
    else:
        # Se a mensagem não for um comando, trata como uma pergunta para a IA (fallback)
        if openai_client:
            await update.message.reply_text("🧠 Analisando sua pergunta com a IA...")
            result = await analisar_com_ia(user_id, text)
            await update.message.reply_text(result)
        else:
            await update.message.reply_text("Comando não reconhecido. Use /start para ver o menu principal ou /help para o guia de uso.")


async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Processa cliques nos botões inline."""
    query = update.callback_query
    await query.answer()
    
    data = query.data
    user_id = query.from_user.id
    
    if data == 'deals':
        result = await buscar_discount_api_real(context)
        await query.edit_message_text(result, parse_mode='Markdown', disable_web_page_preview=True)
        
    elif data == 'ml_search':
        context.user_data['state'] = 'waiting_ml_term'
        await query.edit_message_text("🛒 Por favor, digite o termo que você deseja buscar no Mercado Livre (Ex: 'SSD 1TB').")
        
    elif data == 'add_purchase':
        context.user_data['state'] = 'waiting_purchase'
        await query.edit_message_text(
            "💳 Para registrar uma compra, use o formato:\n"
            "`Produto - Valor - Categoria`\n"
            "Exemplo: `iPhone 15 - 5000 - Eletrônicos`"
        )
        
    elif data == 'ask_ai': # Novo handler para IA
        if not openai_client:
            await query.edit_message_text("❌ A funcionalidade de IA está desativada (chave da OpenAI não configurada).")
            return
        context.user_data['state'] = 'waiting_ai_prompt'
        await query.edit_message_text(
            "🧠 Qual sua pergunta? Você pode pedir uma análise dos seus gastos ou uma dica financeira geral.\n"
            "Ex: 'Em que categoria eu mais gastei?' ou 'Devo comprar um novo celular?'"
        )
        
    elif data == 'my_expenses':
        session = Session()
        purchases = session.query(Purchase).filter_by(user_id=user_id).order_by(Purchase.date.desc()).all()
        session.close()
        
        if not purchases:
            await query.edit_message_text("📊 Você ainda não registrou nenhuma compra no banco de dados.")
            return
            
        total_spent = sum(p.value for p in purchases)
        num_purchases = len(purchases)
        
        message_text = (
            "📊 **Resumo dos Seus Gastos (Persistente)** 📊\n\n"
            f"Total Gasto: **R$ {total_spent:.2f}**\n"
            f"Número de Compras: **{num_purchases}**\n\n"
            "**Últimas Compras Registradas:**\n"
        )
        
        # Mostrar as últimas 5 compras
        for p in purchases[:5]:
            message_text += f" - {p.product} (R$ {p.value:.2f}) - {p.category} ({p.date.strftime('%d/%m')})\n"
            
        await query.edit_message_text(message_text, parse_mode='Markdown')
        
    elif data == 'help':
        await help_command(query, context)
        
def main() -> None:
    """Inicia o bot."""
    if not TELEGRAM_BOT_TOKEN:
        logger.error("TELEGRAM_BOT_TOKEN não encontrado. Verifique seu arquivo .env.")
        return

    application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    # Handlers de comandos
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))

    # Handler de mensagens de texto (para Shopee, Compras e IA)
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    # Handler de cliques em botões inline
    application.add_handler(CallbackQueryHandler(handle_callback))

    logger.info("🚀 Bot iniciado! Pressione Ctrl+C para parar.")
    application.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == '__main__':
    main()
