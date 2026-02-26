**Subject: Replication attempts related to citations in your {{ server_name }} preprint**

Dear {{ author_first_name }} {{ author_last_name }},

Thank you for sharing *{{ preprint_title }}* on {{ server_name }}. We are part of the FORRT team maintaining the Library of Replication Attempts (FLoRA). We have often heard that researchers struggle to find replication studies relevant to the original work they rely on, because replication attempts are not consistently linked to the original studies in common databases. To help bridge this gap, we are reaching out to share potentially relevant replication data based on your preprint references.

Our system has identified potential matches between your preprint references and the FLoRA database, which you might find relevant{% if some_replications_cited %} in addition to the replication{{ 's' if cited_replication_count != 1 else '' }} you are already citing{% endif %}.

---
{% for original in originals %}

**Cited original:**

{{ original.full_reference }}{% if original.doi %} [DOI:{{ original.doi }}]({{ original.doi_url }}){% endif %}

**Replication attempt{{ 's' if original.replications|length > 1 else '' }}:**
{% for replication in original.replications %}
{{ '• ' if original.replications|length > 1 else '' }}{{ replication.full_reference }}{% if replication.doi %} [DOI:{{ replication.doi }}]({{ replication.doi_url }}){% endif %}{% if replication.oa_url %} [Open Access]({{ replication.oa_url }}){% endif %} — reported as {{ replication.outcome }} ([Learn more]({{ flora_learn_more_url }}))
{% endfor %}
---
{% endfor %}

If relevant to your framing and interpretation, you may want to consider citing one or more replication attempts and/or contextualising the original citation(s). Providing this context can help readers better evaluate the current evidence base for these effects.

This message is informational. We are conducting a study to evaluate whether such notifications are useful to researchers.

**One-click feedback: Was this email helpful?**

*You will be able to provide some additional feedback after clicking on the link. By clicking, you consent for your response to be included in our analysis.*

[Helpful]({{ feedback_helpful_url }}) | [Not helpful]({{ feedback_not_helpful_url }}) | [Already aware of all these replications]({{ feedback_already_aware_url }}) | [Report data error/concern]({{ feedback_report_error_url }})

If you have any questions, please reply to this email.

Sincerely,

The FLoRA Team

Dr Lukas Wallrich, Birkbeck, University of London<br>
Dr Lukas Röseler, Münster Center for Open Science, University of Münster<br>
Dr Josefina Weinerova, Birkbeck, University of London<br>
Keegan Vaz, Technical University Dortmund

This is part of the UKRI-funded project *Making Replications Count.* You can find further information on [our website](https://forrt.org/marco/), which includes additional tools to discover replications.

<small>
We might email you about other preprints you publish in the coming months. If you don't want to receive such emails, [unsubscribe from future FLoRA-Notify emails]({{ unsubscribe_mailto }}).

***Data protection notice:** We are using publicly accessible bibliographic information from OSF Preprints and related databases. We are processing only your name and email address associated with your preprint to send you this message. Your data will not be shared. If you chose to share your feedback via the links above, that data will be used for research. For full details, see [link to privacy notice]. We will only send one email per preprint; to unsubscribe from emails concerning future preprints, please use the link above.*
</small>
