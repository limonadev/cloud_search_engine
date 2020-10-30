import random
from english_words import english_words_alpha_set

names_file = open('names/names-from-forbes-wp_users.txt', 'r')

names = []
for line in names_file.readlines():
    line.strip()
    names.append(line.replace('\n',''))

names_file.close()

selected_names = random.sample(names, k=100)
english_words = list(english_words_alpha_set)

for name in selected_names:
    page = ''

    web = f'www.{name}.com'
    web_file = open(f'input/{web}', 'w')

    number_of_lines = random.randint(100, 150)
    for _ in range(number_of_lines):
        number_of_words = random.randint(100, 150)

        line = random.choices(english_words, k=number_of_words)

        should_link = random.choice([True, False])
        if should_link:
            number_of_links = random.randint(1, 2)
            links = random.choices(selected_names, k = number_of_links)
            links = [f'www.{link}.com' for link in links]
            line.extend(links)
        
        random.shuffle(line)
        page = page + ' '.join(line) + '\n'

    web_file.write(page)
    web_file.close()