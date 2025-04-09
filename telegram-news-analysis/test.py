from transformers import pipeline

classifier = pipeline('sentiment-analysis', model='DeepPavlov/rubert-base-cased')

text = "Пример текста для анализа!!вааау очень крутой текст пушка"
result = classifier(text)

print(result)