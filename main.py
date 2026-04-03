from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field

app = FastAPI(title="Contact API")

_contacts: dict[str, dict] = {}


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


@app.get("/contact/{label}", response_model=ContactResponse)
def get_contact(label: str) -> ContactResponse:
    if label not in _contacts:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No contact with label {label!r}",
        )
    data = _contacts[label]
    return ContactResponse(label=label, **data)


@app.post("/contact", response_model=ContactResponse, status_code=status.HTTP_201_CREATED)
def create_contact(body: ContactCreate) -> ContactResponse:
    if body.label in _contacts:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Contact with label {body.label!r} already exists",
        )
    _contacts[body.label] = {"name": body.name, "email": body.email}
    return ContactResponse(
        label=body.label,
        name=body.name,
        email=body.email,
    )


@app.delete("/contact/{label}", status_code=status.HTTP_204_NO_CONTENT)
def delete_contact(label: str) -> None:
    if label not in _contacts:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No contact with label {label!r}",
        )
    del _contacts[label]


@app.put("/contact/{label}", response_model=ContactResponse)
def replace_contact(label: str, body: ContactReplace) -> ContactResponse:
    if label not in _contacts:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No contact with label {label!r}",
        )
    _contacts[label] = {"name": body.name, "email": body.email}
    return ContactResponse(label=label, name=body.name, email=body.email)
