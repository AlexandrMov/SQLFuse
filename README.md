SQLFuse
=======

Что это?
--------
SQLFuse - это файловая система пользовательского режима, основанная на [FUSE](http://fuse.sourceforge.net/), которая отображает объекты сервера SQL на файловую систему: схемы, таблицы, представления, хранимые процедуры, функции, колонки, триггеры и др. Кроме отображения объектов SQLFuse частично поддерживает их создание, редактирование и удаление.

Лицензирование
--------------
О порядке распространения и лицензирования SQLFuse вы можете прочитать в файле LICENSE.

Сборка и установка
------------------
Сборка осуществляется путём вызова команд:
```bash
make clean
make
```

> Установка в систему пока не предусмотрена.


Запуск и настройка
------------------
SQLFuse основана на FUSE, поэтому про опции монтирования вы можете прочитать на [FUSE](http://fuse.sourceforge.net/). Параметры монтирования в опции ```-o``` относящиеся к SQLFuse представлены ниже:
- ```profilename```  - наименование профиля, которое определено в конфигурационном файле `sqlfuse.conf`, который находится в каталоге `conf/` в корневой директории SQLFuse.

Пример монтирования БД AdventureWorks2008R2:
```bash
./sqlfuse -o profilename=AdventureWorks2008R2 ~/myserver/advworks
```


sqlfuse.conf
------------
Конфигурационные файлы задают порядок работы и удобство настройки SQLFuse. Синтаксис имеет сходство с .ini-файлами, и позволяет описать группы, содержащие пары ключ-значение.
Пример использования формата можно найти на сайте freedesktop.org, например:
- [Desktop Entry Specification](http://freedesktop.org/Standards/desktop-entry-spec)
- [Icon Theme Specification](http://freedesktop.org/Standards/icon-theme-spec)

Файл же `sqlfuse.conf` должен включать одну или более групп, имеющие пары ключ-значение. Наименование группы задаёт имя профиля, которое будет искать SQLFuse при монтировании в параметре ```profilename``` опции ```-o```. Существует и зарезервированное наименование группы - `[global]`. Пары ключ-значение группы `[global]` заменяют пары ключ-значение других пользовательские групп. Таким образом, Вы можете задавать сквозные параметры, которые применимы ко всем профилям.

Далее представлен список возможных ключей:
- ```appname``` - задаёт наименование приложения, которое будет передаваться в параметрах подключения к серверу, - по умолчанию SQLFuse;
- ```maxconn``` - устанавливает количество подключений, которое будет постоянно поддерживать SQLFuse, при соединении с сервером SQL (если Вы используете графическую среду на подобие GNOME или KDE для навигации, желательно указать значение 2 или более), ожидает целочисленное значение;
- ```to_codeset```, ```from_codeset``` - конвертирует текст определений модулей сервера SQL заданную кодировку ```to_codeset``` из ```from_codeset``` (должен быть установлен `iconv`), ожидает текстовое значение;
- ```ansi_npw``` - принудительное включение параметров ```QUOTED_IDENTIFIER```, `ANSI_NULLS`, `ANSI_WARNINGS`, `ANSI_PADDINGS`, `CONCAT_NULL_YIELDS_NULL` в состояние `ON`, - необходимо на некоторых старых серверах, и при не верной/необходимой настройки БД сервера, ожидает значения `true` или `false`;
- ```servername``` - имя и адрес экземпляра сервера, к которому необходимо подключиться;
- ```dbname``` - наименование базы данных, которую следует примонтировать к файловой системе;
- ```auth``` - наименование профиля авторизации, который задан в файле `sqlconf.auth.conf` (см. след. раздел);
- ```username``` - логин пользователя БД;
- ```password``` - пароль пользователя БД;
- `default_column` - задаёт определение столбца по умолчанию; могут быть переданы шаблоны `%schema` и `%table`, заменяемые на наименования схемы и таблицы соответственно;
- `merge_names`  - если `%schema` и `%table` одинаковы, то заменить их, а также символы между ними на `%table`, принимает значение `true` или `false`.

> При подключению к экземпляру сервера, например, `test\test`, экранировать символ `\` не нужно, - это делает за Вас SQLFuse, при чтении конфигурационных файлов.

sqlfuse.auth.conf
-----------------
Файл `sqlfuse.auth.conf` также располагается в директории `conf/` проекта SQLFuse и формат определённый freedesktop.org.
Конфигурационный файл определяет профиль авторизации, наименование которого определяет имя группы. Профиль авторизации может присвоен подключению через параметр ```auth``` в файле `sqlfuse.conf`.

Группа профиля авторизации имеет всего два параметра:
- ```username``` - логин пользователя БД;
- ```password``` - пароль пользователя БД.

> Если Вам необходима авторизация в домене, то символ `\` экранировать нет необходимости.


Контакты и обратная связь
-------------------------
- если Вы хотите сообщить о конкретном баге, или оказать посильную помощь в разработке, то вам [сюда](http://sqlfuse.org);
- можете отправлять предложения и замечания напрямую автору sqlfuse@gmail.com.
