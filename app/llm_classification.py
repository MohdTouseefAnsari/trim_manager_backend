import os

from groq import Groq

client = Groq(
    api_key=os.environ.get("GROQ_API_KEY"),
)

import json

def llm_assign(listing, candidate_trims):
    """
    listing: dict with keys like 'trim', 'make', 'model'
    candidate_trims: list of canonical trim strings
    client: Groq/OpenRouter client
    """

    raw_trim = listing.trim
    model = listing.model
    brand = listing.brand

    # Build a prompt
    prompt = f"""
You are a car trim expert, based in the GCC/SAUDI Region.

Given a raw trim input from an ad, pick the closest canonical trim from the list below, using features of the car,
If none of the trims match, output "UNMAPPED":


Make: {brand}
Model: {model}
Raw trim: "{raw_trim}"

Canonical trims:
{chr(10).join(candidate_trims)}

Rules:
- Only choose from the canonical trims.
- Be robust to abbreviations, typos, word order changes, and extra descriptors.
Rules:
- Only choose from the canonical trims.
- Be robust to abbreviations, typos, word order changes, and extra descriptors.
- Respond ONLY in JSON following this format:

Example:
{{
    "trim": "Land Rover Defender V8 P525 Edition",
    "confidence": 0.95,
    "assignment_method": "LLM"
}}

If no match is found:
{{
match : "UNMAPPED"
}}
"""

    # Call the LLM
    response = client.chat.completions.create(
        messages=[
            {"role": "user", "content": prompt}
        ],
        model="llama-3.3-70b-versatile",
        max_tokens=150
    )

    # Extract text
    text = response.choices[0].message.content

    # Parse JSON safely
    try:
        result = json.loads(text)
    except json.JSONDecodeError:
        result = {"match": "UNMAPPED"}

    return result



