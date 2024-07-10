package main

import (
	"database/sql"
	"errors"
	"fmt"
	"log"

	_ "github.com/lib/pq"
)

/*
# Реализация метода Process, который отвечает за логику обновления документов.

* Документ, полученный из очереди и переданный как аргумент, сранивается с его предыдущей версией.
(Метод использует БД для доступа к предыдущим версиям документов)
* Если такого документа еще не было, он добавляется в БД и возвращается, чтобы быть отправленным в другую очередь.
* Иначе, в зависимости от того, новая это версия документа или старая, документ обновляется по некоторому алгоритму.
* Обновленная версия замещает старую в БД и возвращается из метода.
*/
func (d *TDocument) Process() (*TDocument, error) {
	// Открываем новый коннект к бд
	// DATABASE vk_test на localhost
	// TABLE docs
	// docs имеет вид (url serial PRIMARY KEY, pubdate NUMERIC, fetch_time NUMERIC, text TEXT, first_fetch_time NUMERIC)

	db, err := sql.Open("postgres", "postgres://user_vk_test:pass@localhost/vk_test?sslmode=disable")
	if err != nil {
		log.Fatalf("Failed to connect to database: %v\n", err)
		return nil, errors.New("connection to database error")
	}
	defer db.Close()

	// Смотрим на те данные, что уже находятся там
	var current_data *sql.Rows
	current_data, err = db.Query("SELECT url, pubdate, fetch_time, text, first_fetch_time FROM docs WHERE url = $1", d.Url)
	if err != nil {
		log.Printf("Error selecting prev versions of doc: %v\n", err)
		return nil, errors.New("selecting from database error")
	}

	// Если это не первый док с таким Url, то меняем его в соответствии с требованиями
	if current_data.Next() {
		var curr_doc TDocument
		err = current_data.Scan(&curr_doc.Url, &curr_doc.PubDate, &curr_doc.FetchTime, &curr_doc.Text, &curr_doc.FirstFetchTime)
		if err != nil {
			log.Printf("Error getting data from query: %v\n", err)
			return nil, errors.New("reading data from query error")
		}

		fmt.Printf("Current doc in database: %v\n", curr_doc)

		// Если это дубликат, то не пишем ничего в очередь и не обновляем данные в бд.
		// При этом здесь нам осталось сравнить только по FetchTime, тк Url и FetchTime формируют
		// уникальный идентификатор версии, в случае совпадения пар мы и имеем дубликат.
		if curr_doc.FetchTime == d.FetchTime {
			fmt.Printf("Nothing to change\n")
			return nil, nil
		}

		// Если новый док по FetchTime позднее
		if d.FetchTime > curr_doc.FetchTime {
			curr_doc.Text = d.Text
			curr_doc.FetchTime = d.FetchTime
			// PubDate остается тем же
			// FirstFetchTime остается тем же
		} else { // Иначе мы получили версию, которая была раньше
			// Text оставляем более новый
			// FetchTime тоже более новый
			curr_doc.PubDate = d.PubDate
			curr_doc.FirstFetchTime = d.FetchTime
		}

		// Обновляем версию в БД
		_, err = db.Exec("UPDATE docs SET pubdate = $2::NUMERIC, fetch_time = $3::NUMERIC, text = $4::TEXT, first_fetch_time = $5::NUMERIC WHERE Url = $1",
			curr_doc.Url, curr_doc.PubDate, curr_doc.FetchTime, curr_doc.Text, curr_doc.FirstFetchTime)
		if err != nil {
			log.Printf("Error updating doc: %v\n", err)
			return nil, errors.New("updating database error")
		}

		fmt.Printf("Updated doc: %v\n", curr_doc)
		return &curr_doc, nil

	} else {
		// Добавляем док в бд
		// Заполняем FirstFetchTime, тк он изначально отсутствует
		d.FirstFetchTime = d.FetchTime
		_, err = db.Exec("INSERT INTO docs (url, pubdate, fetch_time, text, first_fetch_time) VALUES ($1, $2, $3, $4, $5)", d.Url, d.PubDate, d.FetchTime, d.Text, d.FirstFetchTime)
		if err != nil {
			log.Printf("Error inserting record: %v\n", err)
			return nil, errors.New("inserting to database error")
		}

		return d, nil
	}
}
