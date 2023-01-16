// Delete all nodes and connections.
MATCH (n)
DETACH DELETE n

// Find the top 5 authors with the most publications.
MATCH (p:Person)-[:IS_AUTHOR]->(pub:Publication)
RETURN p.name as AuthorName, p.last_name as AuthorLastName, COUNT(pub) as NumberOfPublications
ORDER BY NumberOfPublications DESC
LIMIT 5

// Find the average number of authors for publications in a specific category.
MATCH (c:Category {code: "stat.CO"})<-[:IS_IN]-(pub:Publication)<-[:IS_AUTHOR]-(p:Person)
WITH pub, COUNT(p) as authors
RETURN AVG(authors)

// Find the most common categories for a specific author.
MATCH (p:Person {name: 'P. C.', last_name: "Canfield"})-[:IS_AUTHOR]->(pub:Publication)-[:IS_IN]->(c:Category)
WITH c, COUNT(pub) as publications
RETURN c.name, publications
ORDER BY publications DESC
LIMIT 5

// Find all co-authors of a specific person, and the number of publications they have co-authored together.
MATCH (p:Person {name: 'P. C.', last_name: "Canfield"})-[:IS_AUTHOR]->(pub:Publication)<-[:IS_AUTHOR]-(coauthor:Person)
WHERE coauthor <> p
RETURN coauthor.last_name as CoAuthor, COUNT(pub) as NumberOfPublications
ORDER BY NumberOfPublications DESC

// Find all the publications of a specific person, and the number of categories each publication is in.
MATCH (p:Person {name: 'P. C.', last_name: "Canfield"})-[:IS_AUTHOR]->(pub:Publication)-[:IS_IN]->(c:Category)
RETURN pub.title as Publication, COUNT(c) as NumberOfCategories
ORDER BY NumberOfCategories DESC

// Find the pair of categories with the most amount of publications in both.
MATCH (cat1:Category)<-[:IS_IN]-(pub:Publication)-[:IS_IN]->(cat2:Category)
WHERE cat1.code <> cat2.code AND cat1.name <> cat2.name
WITH cat1, cat2, COUNT(pub) as commonPublications
RETURN cat1.name as Category1, cat2.name as Category2, commonPublications
ORDER BY commonPublications DESC
LIMIT 5