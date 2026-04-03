import os
from pathlib import Path
from typing import Any, Optional

import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field

app = FastAPI(title="Contact API")

CONF_DIR = Path("/etc/k8rest")


def _get_db_connection():
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST", "localhost"),
            port=int(os.getenv("DB_PORT", "5432")),
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            cursor_factory=RealDictCursor,
        )
    except psycopg2.Error as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database connection failed: {exc.pgerror or str(exc)}",
        )
    return conn


def _ensure_schema() -> None:
    conn = _get_db_connection()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS contacts (
                    label TEXT PRIMARY KEY,
                    name  TEXT NOT NULL,
                    email TEXT
                )
                """
            )
    finally:
        conn.close()


@app.on_event("startup")
def on_startup() -> None:
    _ensure_schema()


class ContactCreate(BaseModel):
    label: str = Field(..., min_length=1, description="Unique key for the contact")
    name: str = Field(default="", description="Display name")
    email: str | None = None


class ContactReplace(BaseModel):
    name: str = ""
    email: str | None = None


class ContactResponse(BaseModel):
    label: str
    name: str
    email: str | None


class ConfFileEntry(BaseModel):
    file_name: str
    content: str


@app.get("/env")
def get_env() -> dict[str, str]:
    return dict(os.environ)


@app.get("/conf", response_model=list[ConfFileEntry])
def get_conf() -> list[ConfFileEntry]:
    if not CONF_DIR.is_dir():
        return []
    out: list[ConfFileEntry] = []
    for path in sorted(CONF_DIR.iterdir()):
        if path.is_file():
            try:
                text = path.read_text(encoding="utf-8", errors="replace")
            except OSError:
                text = ""
            out.append(ConfFileEntry(file_name=path.name, content=text))
    return out


@app.get("/contact/{label}", response_model=ContactResponse)
def get_contact(label: str) -> ContactResponse:
    conn = _get_db_connection()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                "SELECT label, name, email FROM contacts WHERE label = %s",
                (label,),
            )
            row: Optional[dict[str, Any]] = cur.fetchone()
            if row is None:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"No contact with label {label!r}",
                )
            return ContactResponse(**row)
    finally:
        conn.close()


@app.post("/contact", response_model=ContactResponse, status_code=status.HTTP_201_CREATED)
def create_contact(body: ContactCreate) -> ContactResponse:
    conn = _get_db_connection()
    try:
        with conn, conn.cursor() as cur:
            try:
                cur.execute(
                    """
                    INSERT INTO contacts (label, name, email)
                    VALUES (%s, %s, %s)
                    """,
                    (body.label, body.name, body.email),
                )
            except psycopg2.IntegrityError:
                conn.rollback()
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=f"Contact with label {body.label!r} already exists",
                )
            conn.commit()
        return ContactResponse(
            label=body.label,
            name=body.name,
            email=body.email,
        )
    finally:
        conn.close()


@app.delete("/contact/{label}", status_code=status.HTTP_204_NO_CONTENT)
def delete_contact(label: str) -> None:
    conn = _get_db_connection()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                "DELETE FROM contacts WHERE label = %s",
                (label,),
            )
            if cur.rowcount == 0:
                conn.rollback()
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"No contact with label {label!r}",
                )
            conn.commit()
    finally:
        conn.close()


@app.put("/contact/{label}", response_model=ContactResponse)
def replace_contact(label: str, body: ContactReplace) -> ContactResponse:
    conn = _get_db_connection()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                UPDATE contacts
                SET name = %s,
                    email = %s
                WHERE label = %s
                """,
                (body.name, body.email, label),
            )
            if cur.rowcount == 0:
                conn.rollback()
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"No contact with label {label!r}",
                )
            conn.commit()
        return ContactResponse(label=label, name=body.name, email=body.email)
    finally:
        conn.close()
