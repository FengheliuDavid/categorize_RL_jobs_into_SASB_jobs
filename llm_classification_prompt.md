You are an ESG expert. Classify the job role below into SASB categories based on its PRIMARY responsibilities.

## SASB Categories

1. GHG_Emissions – Addresses direct (Scope 1) greenhouse gas emissions from a company's own stationary sources (factories, power plants) and mobile sources (trucks, planes), including non-combusted releases from natural resource extraction or land use. Also covers management of regulatory compliance and reputational risks related to GHG emissions. The seven Kyoto Protocol GHGs are in scope (CO2, CH4, N2O, HFCs, PFCs, SF6, NF3).

2. Air_Quality – Addresses management of air pollutants (NOx, SOx, VOCs, heavy metals, particulate matter) from stationary and mobile industrial sources. Does not include GHG emissions, which are covered separately.

3. Energy_Management – Addresses a company's management of energy consumption from external utility providers (grid energy), including energy efficiency, energy mix, and grid reliance. Covers energy use in manufacturing and service provision; excludes upstream supplier or downstream product-use energy.

4. Water_&_Wastewater_Management – Addresses a company's water use, consumption, wastewater generation, and impacts on local water resources. Covers strategies for water efficiency, recycling, wastewater treatment, and discharge management including groundwater protection.

5. Waste_&_Hazardous_Materials_Management – Addresses management of solid, hazardous, and non-hazardous waste generated in manufacturing, agriculture, and industrial processes. Covers treatment, storage, disposal, and regulatory compliance; excludes air emissions and wastewater, which are covered separately.

6. Ecological_Impacts – Addresses a company's impacts on ecosystems and biodiversity through land use, natural resource extraction, construction, and site development. Covers biodiversity loss, habitat destruction, and deforestation across all project stages; excludes climate change impacts on ecosystems.

7. Human_Rights_&_Community_Relations – Addresses a company's management of its relationships with local communities, including human rights impacts, indigenous peoples' rights, socio-economic effects, and community engagement. Does not cover environmental impacts like air pollution, which are addressed in separate categories.

8. Customer_Privacy – Addresses risks from using personally identifiable information (PII) for secondary purposes such as targeted marketing, including consent management and evolving privacy regulation. Excludes cybersecurity risks, which are covered in Data_Security.

9. Data_Security – Addresses management of risks from data breaches and unauthorized access to sensitive customer or proprietary data. Covers IT infrastructure, staff training, record keeping, and cooperation with law enforcement to ensure data security.

10. Access_&_Affordability – Addresses a company's ability to ensure broad, affordable access to its products and services for underserved markets and populations. Relevant sectors include healthcare, financial services, utilities, education, and telecommunications.

11. Product_Quality_&_Safety – Addresses unintended product characteristics that create health or safety risks for end-users. Covers product liability, recalls, product testing, and ingredient or content management.

12. Customer_Welfare – Addresses customer welfare concerns inherent to product design and delivery, such as health and nutrition of food products, antibiotic use in animal production, and management of controlled substances. Distinct from Product_Quality_&_Safety, which covers unintended safety failures rather than inherent product qualities.

13. Selling_Practices_&_Product_Labeling – Addresses transparency and accuracy in marketing, advertising, and product labeling. Covers deceptive or predatory selling practices, misleading labels, and ethical marketing standards.

14. Labor_Practices – Addresses compliance with labor laws and international labor standards, including fair wages, workers' rights, and prohibition of child or forced labor. Also covers employee benefits, workforce attraction and retention, and relations with organized labor.

15. Employee_Health_&_Safety – Addresses a company's ability to maintain a safe and healthy workplace free of injuries, fatalities, and illness. Covers safety management systems, training, audits, personal protective equipment, and mental health programs.

16. Employee_Engagement,_Diversity_&_Inclusion – Addresses a company's efforts to build a diverse, inclusive workforce through equitable hiring and promotion practices. Covers discrimination on the bases of race, gender, ethnicity, religion, sexual orientation, and other factors.

17. Product_Design_&_Lifecycle_Management – Addresses incorporation of ESG factors into product design, packaging, distribution, and end-of-life management. Covers companies' ability to meet societal demand for sustainable products and comply with evolving environmental regulations; excludes direct operational impacts, which are covered elsewhere.

18. Business_Model_Resilience – Addresses an industry's capacity to adapt its business model to environmental, social, and political transitions, including the shift to a low-carbon economy. Highlights industries where evolving realities may fundamentally challenge existing business models.

19. Supply_Chain_Management – Addresses management of ESG risks (environmental impacts, human rights, labor practices, ethics) created by suppliers through their operations. Covers supplier screening, monitoring, and engagement; excludes external risks to suppliers such as climate change, which is covered in Materials_Sourcing_&_Efficiency.

20. Materials_Sourcing_&_Efficiency – Addresses resilience of materials supply chains to external risks such as climate change, which affect supplier operations and resource availability. Covers resource efficiency, use of recycled/renewable materials, dematerialization, and supplier engagement to manage these external risks.

21. Physical_Impacts_of_Climate_Change – Addresses a company's ability to manage physical climate risks (extreme weather, sea-level rise, shifting climate) to its owned or controlled assets and operations. Also covers incorporation of climate considerations into products and services such as insurance underwriting or real estate planning.

22. Business_Ethics – Addresses a company's management of ethical conduct risks including fraud, corruption, bribery, and conflicts of interest. Covers policies, training, and procedures to ensure services meet the highest professional and ethical standards.

23. Competitive_Behavior – Addresses legal and social risks from monopolistic or anti-competitive practices, including price fixing, collusion, and misuse of bargaining power. Covers management of antitrust compliance and intellectual property protection.

24. Management_of_the_Legal_&_Regulatory_Environment – Addresses a company's engagement with regulators, reliance on subsidies or favorable regulation, and lobbying activities. Covers compliance with relevant regulations and the alignment of management and investor views on regulatory engagement.

25. Critical_Incident_Risk_Management – Addresses management systems and scenario planning to prevent or minimize low-probability, high-impact accidents and emergencies with significant environmental or social consequences. Covers safety culture, technological controls, and long-term organizational resilience.

26. Systemic_Risk_Management – Addresses a company's contributions to and management of systemic risks from large-scale failures of financial, natural resource, or technological systems. For financial institutions, also covers ability to absorb shocks from economic stress and meet regulatory requirements related to systemic complexity.

## Instructions

- A role qualifies ONLY if the SASB issue is the **reason the role exists** — the role was created specifically to manage that area
- Do NOT classify a role just because the employing company operates in a relevant industry, or because the role occasionally touches that topic
- Most roles map to 0–2 categories; when in doubt, return `[]` — a false negative is better than a false positive

**The key test:** Would this role be listed on an ESG/sustainability job board? Would its job posting contain sustainability, compliance, or safety language as the *primary* function?

**Common false positives to avoid:**
- `Customer Service` / `Customer Support` → NOT Customer_Privacy (they handle data but don't govern privacy policy)
- `Sales Representative` / `Account Manager` → NOT Selling_Practices (they sell but don't govern selling practices)
- `Marketing` / `Content Creator` / `Brand Manager` → NOT Selling_Practices (they produce content but don't set labeling standards)
- `Banker` / `Loan Officer` / `Financial Advisor` → NOT Access_&_Affordability (they provide services but don't govern access programs)
- `Lawyer` / `Legal Assistant` / `Compliance Analyst` → NOT Business_Ethics (legal work ≠ managing the company's ethics program)
- `Engineer` / `Developer` / `Architect` (generic) → NOT Supply_Chain or Materials (they build things but don't manage ESG supplier risk)
- `HR` / `Recruiter` / `Talent Acquisition` → NOT Labor_Practices or Employee_Engagement (they execute HR processes but don't govern labor standards)
- `Operations Manager` / `Project Manager` (generic) → 0 categories unless the role explicitly names an ESG function
- Any research, academic, or administrative support role → 0 categories regardless of subject matter

**Roles that DO qualify (examples):**
- `EHS Manager` / `Safety Officer` / `HSE Engineer` → Employee_Health_&_Safety ✓
- `Emissions Engineer` / `GHG Inventory Analyst` → GHG_Emissions ✓
- `Wastewater Treatment Operator` / `Water Resources Manager` → Water_&_Wastewater_Management ✓
- `Hazardous Waste Coordinator` / `Environmental Compliance Specialist` → Waste_&_Hazardous_Materials_Management ✓
- `Chief Privacy Officer` / `Privacy Counsel` → Customer_Privacy ✓
- `Cybersecurity Analyst` / `Information Security Manager` → Data_Security ✓
- `Sustainability Manager` / `ESG Analyst` → multiple categories depending on scope ✓
- `Diversity & Inclusion Manager` → Employee_Engagement,_Diversity_&_Inclusion ✓

## Output

Return ONLY a JSON object, no explanation:
{"sasb_categories": ["Category_Name"], "confidence": "high|medium|low"}

If no categories apply: {"sasb_categories": [], "confidence": "high"}

## Job Role

{role_label}
