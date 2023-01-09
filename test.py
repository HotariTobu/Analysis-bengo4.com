import spacy
import ginza

nlp = spacy.load('ja_ginza')
ginza.set_split_mode(nlp, 'C')

doc = nlp('依存構造文の解析をGinzaで行います。')
for sent in doc.sents:
    for token in sent:
        print(token.i, token.orth_, token.lemma_, token.pos_, token.tag_, token.dep_, token.head.i)
    print('EOS')