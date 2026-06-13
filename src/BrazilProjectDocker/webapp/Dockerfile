# 1. Sá»­ dá»¥ng mÃ´i trÆ°á»ng Python báº£n nháº¹ (slim) Ä‘á»ƒ tá»‘i Æ°u dung lÆ°á»£ng
FROM python:3.9-slim

# 2. Táº¡o thÆ° má»¥c lÃ m viá»‡c áº£o bÃªn trong Container tÃªn lÃ  /app
WORKDIR /app

# 3. CÃ i Ä‘áº·t cÃ¡c thÆ° viá»‡n cáº§n thiáº¿t trá»±c tiáº¿p thÃ´ng qua pip
RUN pip install --no-cache-dir flask kafka-python

# 4. Copy toÃ n bá»™ mÃ£ nguá»“n (app.py vÃ  thÆ° má»¥c templates) vÃ o trong /app
COPY . .

# 5. Khai bÃ¡o má»Ÿ cá»•ng 5000 cho Website
EXPOSE 5000

# 6. Lá»‡nh sáº½ Ä‘Æ°á»£c thá»±c thi khi Container khá»Ÿi Ä‘á»™ng
CMD ["python", "app.py"]