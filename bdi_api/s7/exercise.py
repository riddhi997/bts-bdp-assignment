from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel

from neo4j import GraphDatabase
from bdi_api.settings import Settings

settings = Settings()

s7 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s7",
    tags=["s7"],
)


class PersonCreate(BaseModel):
    name: str
    city: str
    age: int


class RelationshipCreate(BaseModel):
    from_person: str
    to_person: str
    relationship_type: str = "FRIENDS_WITH"


@s7.post("/graph/person")
def create_person(person: PersonCreate) -> dict:
    """Create a person node in Neo4J.

    Use the BDI_NEO4J_URL environment variable to configure the connection.
    Start Neo4J with: make neo4j
    """
    # TODO: Connect to Neo4J using neo4j.GraphDatabase.driver(settings.neo4j_url, auth=(settings.neo4j_user, settings.neo4j_password))
    # TODO: Create a Person node with the given properties
    # TODO: Return {"status": "ok", "name": person.name}
    driver = GraphDatabase.driver(
        settings.neo4j_url,
        auth=(settings.neo4j_user, settings.neo4j_password)
    )
    with driver.session() as session:
        session.run(
            "CREATE (p:Person {name: $name, city: $city, age: $age})",
            name=person.name, city=person.city, age=person.age
        )
    driver.close()
    return {"status": "ok", "name": person.name}


@s7.get("/graph/persons")
def list_persons() -> list[dict]:
    """List all person nodes.

    Each result should include: name, city, age.
    """
    # TODO: Connect to Neo4J
    # TODO: MATCH (p:Person) RETURN p
    # TODO: Return list of dicts with name, city, age
    driver = GraphDatabase.driver(
        settings.neo4j_url,
        auth=(settings.neo4j_user, settings.neo4j_password)
    )
    with driver.session() as session:
        result = session.run("MATCH (p:Person) RETURN p")
        persons = [{"name": r["p"]["name"], "city": r["p"]["city"], "age": r["p"]["age"]} for r in result]
    driver.close()
    return persons


@s7.get("/graph/person/{name}/friends")
def get_friends(name: str) -> list[dict]:
    """Get friends of a person.

    Returns all persons connected by a FRIENDS_WITH relationship (any direction).
    If person not found, return 404.
    """
    # TODO: Connect to Neo4J
    # TODO: First check if person exists, return 404 if not
    # TODO: MATCH (p:Person {name: name})-[:FRIENDS_WITH]-(friend:Person)
    # TODO: Return list of friend dicts with name, city, age
    driver = GraphDatabase.driver(
        settings.neo4j_url,
        auth=(settings.neo4j_user, settings.neo4j_password)
    )
    with driver.session() as session:
        # Check person exists
        person = session.run("MATCH (p:Person {name: $name}) RETURN p", name=name).single()
        if not person:
            raise HTTPException(status_code=404, detail=f"Person '{name}' not found")
        # Get friends in any direction
        result = session.run(
            "MATCH (p:Person {name: $name})-[:FRIENDS_WITH]-(friend:Person) RETURN friend",
            name=name
        )
        friends = [{"name": r["friend"]["name"], "city": r["friend"]["city"], "age": r["friend"]["age"]} for r in result]
    driver.close()
    return friends


@s7.post("/graph/relationship")
def create_relationship(rel: RelationshipCreate) -> dict:
    """Create a relationship between two persons.

    Both persons must exist. Returns 404 if either is not found.
    """
    # TODO: Connect to Neo4J
    # TODO: Verify both persons exist
    # TODO: CREATE (a)-[:FRIENDS_WITH]->(b)
    # TODO: Return {"status": "ok", "from": rel.from_person, "to": rel.to_person}
    driver = GraphDatabase.driver(
        settings.neo4j_url,
        auth=(settings.neo4j_user, settings.neo4j_password)
    )
    with driver.session() as session:
        a = session.run("MATCH (p:Person {name: $name}) RETURN p", name=rel.from_person).single()
        b = session.run("MATCH (p:Person {name: $name}) RETURN p", name=rel.to_person).single()
        if not a:
            raise HTTPException(status_code=404, detail=f"Person '{rel.from_person}' not found")
        if not b:
            raise HTTPException(status_code=404, detail=f"Person '{rel.to_person}' not found")
        session.run(
            "MATCH (a:Person {name: $from_name}), (b:Person {name: $to_name}) MERGE (a)-[:FRIENDS_WITH]->(b)",
            from_name=rel.from_person, to_name=rel.to_person
        )
    driver.close()
    return {"status": "ok", "from": rel.from_person, "to": rel.to_person}


@s7.get("/graph/person/{name}/recommendations")
def get_recommendations(name: str) -> list[dict]:
    """Get friend recommendations for a person.

    Recommend friends-of-friends who are NOT already direct friends.
    Return them sorted by number of mutual friends (descending).
    If person not found, return 404.

    Each result should include: name, city, mutual_friends (count).
    """
    # TODO: Connect to Neo4J
    # TODO: First check if person exists, return 404 if not
    # TODO: Find friends-of-friends not already friends
    # TODO: Count mutual friends and sort descending
    driver = GraphDatabase.driver(
        settings.neo4j_url,
        auth=(settings.neo4j_user, settings.neo4j_password)
    )
    with driver.session() as session:
        # Check person exists
        person = session.run("MATCH (p:Person {name: $name}) RETURN p", name=name).single()
        if not person:
            raise HTTPException(status_code=404, detail=f"Person '{name}' not found")
        # Find friends of friends not already friends
        result = session.run("""
            MATCH (p:Person {name: $name})-[:FRIENDS_WITH]->(friend)-[:FRIENDS_WITH]->(rec)
            WHERE rec <> p
            AND NOT (p)-[:FRIENDS_WITH]->(rec)
            RETURN rec.name as name, rec.city as city, COUNT(friend) as mutual_friends
            ORDER BY mutual_friends DESC
        """, name=name)
        recommendations = [{"name": r["name"], "city": r["city"], "mutual_friends": r["mutual_friends"]} for r in result]
    driver.close()
    return recommendations
