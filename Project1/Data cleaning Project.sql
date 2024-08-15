-- Data Cleaning Project by Chiaburu Vladislav
-- Есть таблица layoffs с столбцами company, location, industry, total_laid_off,
-- percentage_laid_off, date, stage, country, funds_raised_millions и информация в них
-- с помощью которой я буду совершать манипулирование и трансформирование данных
-- Шаги в данном проекте:
-- 1. Remove duplicates, Первый шаг
-- 2. Standardizing Data, Второй шаг
-- 3. Null Values or blank values, Третий шаг
-- 4. Remove Any Columns, Четвертый шаг

-- 1. Remove duplicates, Первый шаг
SELECT * FROM layoffs; -- Вывожу все данные с таблицы layoffs

CREATE TABLE layoffs_staging LIKE layoffs; -- Создаю 2 таблицу layoffs_staging для манипулирования данными, а 1 таблицу layoffs оставляю как backup
-- Создается таблица с одинаковыми столбцами благодаря LIKE, но без информации в них

INSERT layoffs_staging SELECT * FROM layoffs; -- Ввожу все данные с таблицы layoffs в таблицу layoffs_staging 

-- Собираюсь удалить все имеющиеся дубликаты
SELECT *,  -- Вывожу все столбцы и информацию с таблицы layoffs_staging и столбец row_num, который будет проверять данные благодаря OVER(PARTITION BY по столбцам) и возвращать row_number(номер ряда)
ROW_NUMBER() OVER( -- В случае одинаковых значений во всех столбцах, row_num будет = 2, 3 и так далее
PARTITION BY company, location, 
industry, total_laid_off, percentage_laid_off, `date`, stage,
country, funds_raised_millions) AS row_num
FROM layoffs_staging;


WITH duplicate_rows AS -- Используя subquery и переменную duplicate_rows, вывожу все столбцы с информацией где row_num > 1, то есть все дубликаты
(SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, 
industry, total_laid_off, percentage_laid_off, `date`, stage,
country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_rows
WHERE row_num > 1;

-- Увидев какие дубликаты есть в таблице layoffs_staging, создаю еще одну таблицу layoffs_staging2 с которой буду работать
-- Нажав правой кнопкой мыши на таблицу layoffs_staging > copy to clipboard > create statement > ctrl + v
-- В конце нашей новой таблицы layoffs_staging2 добавляю row_num INT, для того чтобы создать новый столбец и с помощью него удалить все дубликаты
CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
		`row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Создается таблица layoffs_staging2 только со столбцами без информации в них
select * from layoffs_staging2; -- Вывожу всю информацию с таблицы layoffs_staging2 и вижу новый столбец row_num

INSERT INTO layoffs_staging2 -- Добавляю в таблицу layoffs_staging2 всю информации по столбцам
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, 
industry, total_laid_off, percentage_laid_off, `date`, stage,
country, funds_raised_millions) AS row_num
FROM layoffs_staging;

SELECT * -- Вывожу все столбцы с информацией таблицы layoffs_stagins2 где row_num > 1, то есть дубликаты
FROM layoffs_staging2
WHERE row_num > 1;

SET SQL_SAFE_UPDATES = 0; -- Для того чтобы MySQL разрешил удаления данных или же в настройках меняю сам (Edit > Preferences > SQL Editor > Safe Updates)

DELETE -- Удаляем дубликаты
FROM layoffs_staging2
WHERE row_num > 1;

SELECT * FROM layoffs_staging2; -- Вывожу все столбцы с информацией таблицы layoffs_staging2 и вижу что значение row_num осталось = 1
-- Дубликаты удалены


-- 2. Standardizing Data, Второй шаг
SELECT company, TRIM(company) FROM layoffs_staging2; -- Вывожу столбец company и TRIM(company) с таблицы layoffs_staging2 для проверки если нужно удалить свободное пространство
-- Вижу что действительно нужно удалить

UPDATE layoffs_staging2 -- Обновляю значение данных столбца company на TRIM(company)
SET company = TRIM(company);

-- Проверяю если в названии industry есть: NULL/BLANK значения, похожие значения 
SELECT DISTINCT industry FROM layoffs_staging2 -- Вывожу уникальные значения столбца industry с таблицы layoffs_staging2 order by 1
ORDER BY 1; -- и вижу что есть NULL/BLANK значение и похожие значения 'Crypto', 'Crypto Currency', 'CryptoCurrency'

SELECT * FROM layoffs_staging2 -- Благодаря LIKE, вывожу все столбцы с таблицы layoffs_staging2 в котором столбец industry имеет в начале названия слово 'Crypto'
WHERE industry LIKE 'Crypto%';  -- Вижу 3 значения 'Crypto Currency', 'CryptoCurrency' которые нужно поменять на 'Crypto'

UPDATE layoffs_staging2 -- Обновляю значения 'Crypto Currency', 'CryptoCurrency' на 'Crypto'
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT industry FROM layoffs_staging2; -- Вывожу уникальные значение столбца industry и проверяю обновленное значение 'Crypto'

-- Проверяю остальные столбцы с таблицы layoffs_staging2 на наличие каких-либо ошибок
SELECT DISTINCT location FROM layoffs_staging2 ORDER BY 1; -- Вывожу уникальные значения столбца location и вижу ошибки в названии 'MalmГ¶' и 'DГјsseldorf'

UPDATE layoffs_staging2 -- Обновляю значение 'MalmГ¶' на 'Malmo'
SET location = 'Malmo'
WHERE location LIKE 'MalmГ¶';

UPDATE layoffs_staging2 -- Обновляю значение 'DГјsseldorf' на 'Dusseldorf'
SET location = 'Dusseldorf'
WHERE location LIKE 'DГјsseldorf';

SELECT DISTINCT location FROM layoffs_staging2 ORDER BY 1; -- Вывожу уникальные значение столбца location и не вижу больше ошибок

-- Проверяю остальные столбцы с таблицы layoffs_staging2 на наличие каких-либо ошибок
SELECT DISTINCT country FROM layoffs_staging2 ORDER BY 1; -- Вывожу уникальные значения столбца country и вижу ошибку в названии столбца country: 'United States', 'United States.'

UPDATE layoffs_staging2 -- Обновляю значение  'United States.' на 'United States' благодаря TRIM + TRAILING(удаляет указанный символ с правой стороны) в столбце country
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

-- Второй вариант как можно было исправить ошибку:
-- UPDATE layoffs_staging2 
-- SET country = 'United States'
-- WHERE country LIKE 'United States%';

SELECT DISTINCT country FROM layoffs_staging2 ORDER BY 1; -- Вывожу уникальные значение столбца country и не видим больше ошибок

-- Проверяю остальные столбцы с таблицы layoffs_staging2 на наличие каких-либо ошибок
DESC layoffs_staging2; -- Вижу что тип данных(data type) столбца date стоит TEXT, а нужно изменить на DATE

SELECT `date` FROM layoffs_staging2; -- Вывожу столбец date и вижу что значения в другом формате(date format)

-- Для начала нужно изменить формат значений(date format) month/day/year на year-month-day используя STR_TO_DATE
UPDATE layoffs_staging2 -- Обновляю столбец date на указанный формат используя STR_TO_DATE
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

SELECT `date` FROM layoffs_staging2; -- Вывожу данные столбца date для проверки формата

-- Теперь же я могу поменять тип данных(data type) на date используя ALTER TABLE + MODIFY
ALTER TABLE layoffs_staging2
MODIFY `date` DATE;

DESC layoffs_staging2; -- Формат столбца date успешно изменен


-- 3. Null Values or blank values, Третий шаг
SELECT * FROM layoffs_staging2 -- Вывожу все столбцы таблицы layoffs_staging2 в которых столбец industry IS NULL или '' значений
WHERE industry IS NULL OR industry = ''; -- Выводится 4 компании: Airbnb, Bally's Interactive, Carvana, Juul

SELECT * -- Используя JOIN, подключаю таблицы и проверяю если есть недостающие данные в столбцах(смотрю на industry) чтобы добавить и не осталось NULL или '' значений
-- В случае если company встречается более одного раза и в одном ряду industry имеет NULL или '' значения, а в другом имеет значения то сопоставляю значение
FROM layoffs_staging2 AS t1 -- Вижу что Airbnb, Carvana, Juul я могу дополнить столбец industry, но в Bally's Interactive industry столбец имеет значение ''
JOIN layoffs_staging2 AS t2
	ON t1.company = t2.company -- Так как значение столбца location одинаковые не использую доп.сравнение t1.location = t2.location
WHERE (t1.industry IS NULL OR t1.industry = ''); -- То есть компания находится в одной локации а не в разных, в идеале, нужно было бы добавить доп.сравнение но так как это будет лишним - не использовалось


-- Для того чтобы сопоставить значения столбца industry в столбцы где отсутствует информация нужно для начала изменить значения '' на NULL
UPDATE layoffs_staging2 -- Меняю все значения '' на NULL значения столбца industry
SET industry = NULL
WHERE industry = '';

UPDATE layoffs_staging2 AS t1 -- Используя JOIN подключаю таблицы и сопоставляю значения рядов столбца industry: Airbnb - Travel, Carvana - Transportation, Juul - Consumer
JOIN layoffs_staging2 AS t2 -- Из-за того что Bally's Interactive встречается только 1 раз, то значение столбца industry не будет сопоставлено и останется NULL
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

SELECT * FROM layoffs_staging2 -- Вывожу данные где столбец industry имеет NULL или '' значение
WHERE industry IS NULL OR industry = ''; -- Осталось только компания Bally's Interactive с NULL значением


-- 4. Remove Any Columns, Четвертый шаг

SELECT * FROM layoffs_staging2; -- Вывожу все данные с таблицы layoffs_staging2 и вижу что в столбцах total_laid_off и percentage_laid_off есть значения NULL

DELETE -- Удаляю ряды где в ДВУХ столбцах total_laid_off и percentage_laid_off значения NULL, потому что для манипулирования данными они не пригодны
FROM layoffs_staging2 -- Нужно хотя бы чтоб один столбец был заполнен какой-либо информацией
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT * FROM layoffs_staging2; -- Вывожу все данные с таблицы layoffs_staging2 и вижу что остался столбец row_num  который я создал ранее для удаления дубликатов

ALTER TABLE layoffs_staging2 -- Удаляю столбец row_num
DROP row_num;

SELECT * FROM layoffs_staging2; -- Вывожу все данные для перепроверки
