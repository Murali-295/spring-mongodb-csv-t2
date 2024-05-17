package com.mk.controller;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.bson.Document;
import org.bson.json.JsonObject;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

@RestController
public class CsvController {

	private MongoClient mongoClient = MongoClients.create();
	private MongoDatabase mongoDatabase;

	@GetMapping("/get")
	public String store() throws CsvValidationException, IOException {

		// FileReader reader = new FileReader("MOVIES.csv");
		CSVReader csvReader = new CSVReader(new FileReader("MOVIES.csv"));
		String[] header = csvReader.readNext();

		for (String string : header) {

			System.out.println(string);
		}

		String[] data;
		while ((data = csvReader.readNext()) != null) {
			mongoDatabase = mongoClient.getDatabase("movies");
			Document document = new Document();

			Map<String, String> map = new HashMap<>();
			for (int i = 0; i < data.length; i++) {
				document.put(header[i], data[i]);
			}
			mongoDatabase.getCollection(document.getString("title").replace(".", "_").replace("$", "_"))
					.insertOne(document);
		}
		return "Data stored";
	}

	@GetMapping("/returnData/{title}")
	public List<Document> getMovie(@PathVariable String title) {
		mongoDatabase = mongoClient.getDatabase("movies");
		// to store the data and return to user
		List<Document> documents = new ArrayList<>();
		MongoCollection<Document> document = mongoDatabase.getCollection(title);
		// iterating over document to add data to list
		for (Document doc : document.find()) {
			doc.remove("_id");
			documents.add(doc);
		}
		return documents;
	}

	@DeleteMapping("/delete/{title}")
	public String deleteMovie(@PathVariable String title) {
		mongoDatabase = mongoClient.getDatabase("movies");
		MongoCollection<Document> documents = mongoDatabase.getCollection(title);
		documents.drop();
		return "Movie deleted successfully...";
	}

	@PostMapping("/add")
	public String addMovie(@RequestBody Document document) {
		mongoDatabase = mongoClient.getDatabase("movies");
		MongoCollection<Document> documents = mongoDatabase.getCollection(document.getString("title"));
		documents.insertOne(document);
		return "Movie added successfully...";
	}
}
