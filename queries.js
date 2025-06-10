const { MongoClient } = require('mongodb');
const uri = 'mongodb://localhost:27017';

async function runQueries(){
    const client = new MongoClient(uri);

    try{
        await client.connect();
        const db = client.db('plp_bookstore');
        const books = db.collection('books')

    // Task  1
    // Find all books in a specific genre
    const adventureBooks = await books.find({ genre:'Adventure'}).toArray();
    console.log('Adventure Books;', adventureBooks);

    // Find books published after a certain year (1920)
    const booksAfter1920 = await books.find({ published_year: { $gt: 1920 }}).toArray();
    console.log('Books After 1920:', booksAfter1920);

    //Find books by a specific author (George Orwell)
    const GeorgeBooks = await books.find({ author: 'George Orwell'}).toArray();
    console.log('Books by George:', GeorgeBooks);

    //Update the price of a specific book
     await books.updateOne(
      { title: 'The Great Gatsby' },
      { $set: { price: 500 } }
    );
    console.log('Updated price of The Great Gatsby.');

    //Delete a book by its title
    await books.deleteOne({ title: '1984' });
    console.log('Deleted book titled 1984.');

    //Task 2
    //Write a query to find books that are both in stock and published after 2010
    //use projection to return only the title, author, and price fields in your queries
    const filteredBooksAsc = await books.find(
      {
        inStock: true,
        published_year: { $gt: 2010 }
      },
      {
        projection: { title: 1, author: 1, price: 1 }
      }
      // Implement sorting to display books by price (ascending)
      // Use the `limit` and `skip` methods to implement pagination (5 books per page)
    ).sort({ price: 1 })
    .skip(0)
    .limit(5)
    .toArray();

    console.log('Filtered Books Ascending:', filteredBooksAsc);

     const filteredBooksDesc = await books.find(
      {
        inStock: true,
        published_year: { $gt: 2010 }
      },
      {
        projection: { title: 1, author: 1, price: 1 }
      }
      //Implement sorting to display books by price (descending)
      //Use the `limit` and `skip` methods to implement pagination (5 books per page)
    ).sort({ price: -1 })
    .skip(0)
    .limit(5)
    .toArray();

    console.log('Filtered Books Descending:', filteredBooksDesc);
    //Create an aggregation pipeline to calculate the average price of books by genre

    const AvgPriceByGenre = await books.aggregate([
      {
        $group: {
          _id: '$genre',
          averagePrice: { $avg: '$price' }
        }
      }
    ]).toArray();
    console.log('Average Price by Genre:', AvgPriceByGenre); 

    //Create an aggregation pipeline to find the author with the most books in the collection
    const AuthorMostBooks = await books.aggregate([
      {
        $group: {
          _id: 'author',
          bookCount: { $sum: 1 }
        }
      }
    ]).toArray();
    console.log('Author with Most Books:', AuthorMostBooks);

    // Implement a pipeline that groups books by publication decade and counts them
    const BooksByDecade = await books.aggregate([
      {
        $group: {
          _id: { $floor: { $divide: ['$published_year', 10] } },
          count: { $sum: 1 }
        }
      },
      {
        $project: {
          decade: { $multiply: ['$_id', 10] },
          count: 1,
          _id: 0
        }
      }
    ]).toArray();
    console.log('Books by Decade:', BooksByDecade);
    
    //Task 5: Indexing 
    //Create an index on the `title` field for faster searches
    await books.createIndex({ title: 1 });
    console.log('Index created on title');

    //Create a compound index on `author` and `published_year
    await books.createIndex({ author: 1, published_year: -1 });
    console.log('Compound index created on author and published_year');

    // Use the `explain()` method to demonstrate the performance improvement with your indexes
    const explainResult = await books.find({ title: 'The Great Gatsby' }).explain();
    console.log('Explain Result:', explainResult.executionStats);

     } catch (err) {
    console.error('Error:', err);
  } finally {
    await client.close();
  }
}

runQueries();
