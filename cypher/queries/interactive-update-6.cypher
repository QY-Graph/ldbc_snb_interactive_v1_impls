OPTIONAL MATCH
  (author:Person {id: $authorPersonId}),
  (country:Country {id: $countryId}),
  (forum:Forum {id: $forumId})
WITH
  1/count(author) AS testWhetherAuthorFound,
  1/count(country) AS testWhetherCountryFound,
  1/count(forum) AS testWhetherForumFound
MATCH
  (author:Person {id: $authorPersonId}),
  (country:Country {id: $countryId}),
  (forum:Forum {id: $forumId})
CREATE (author)<-[:HAS_CREATOR]-(p:Post:Message {
    id: $postId,
    creationDate: $creationDate,
    locationIP: $locationIP,
    browserUsed: $browserUsed,
    language: $language,
    content: CASE $content WHEN '' THEN NULL ELSE $content END,
    imageFile: CASE $imageFile WHEN '' THEN NULL ELSE $imageFile END,
    length: $length
  })<-[:CONTAINER_OF]-(forum), (p)-[:IS_LOCATED_IN]->(country)
WITH p
UNWIND $tagIds AS tagId
  MATCH (t:Tag {id: tagId})
  CREATE (p)-[:HAS_TAG]->(t)
