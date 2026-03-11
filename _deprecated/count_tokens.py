"""
Simple token counter for SASB classification prompt
"""
import tiktoken

# Read the prompt
with open('llm_classification_prompt_short.md', 'r', encoding='utf-8') as f:
    prompt = f.read()

# Create example batch of 10 roles
example_roles = [
    "1. Software Engineer",
    "2. Data Scientist",
    "3. Product Manager",
    "4. Sales Representative",
    "5. Marketing Manager",
    "6. Financial Analyst",
    "7. Human Resources Manager",
    "8. Operations Manager",
    "9. Customer Service Representative",
    "10. Administrative Assistant"
]
roles_text = "\n".join(example_roles)

# Replace placeholder with batch
prompt = prompt.replace("{job_roles}", roles_text)

# Count tokens
encoding = tiktoken.encoding_for_model("gpt-4")
tokens = len(encoding.encode(prompt))

print(f"Tokens per batch (10 roles): {tokens:,}") # 3,574 / 1259
print(f"\nFor 15,000 classifications (1,500 batches):") # 5,361,000 / 1,888,500
print(f"  Input tokens:  {tokens * 1500:,}")
print(f"  Output tokens (estimated): {500 * 1500:,}")  # ~50 tokens per role × 10 roles
print(f"  Total tokens:  {(tokens * 1500) + (500 * 1500):,}") # 6,111,000 / 2,638,500

