import json


def parse_dashboard_json(file_path):
    with open(file_path, 'r') as file:
        dashboard_data = json.load(file)

    grafana_charts = {}
    parse_panels(dashboard_data['panels'], grafana_charts)

    return grafana_charts


def parse_panels(panels, grafana_charts, section_title=None):
    if section_title != None and section_title not in grafana_charts:
        grafana_charts[section_title] = []
    for panel in panels:
        if panel['type'] == 'row':
            parse_panels(panel['panels'], grafana_charts,
                         section_title=panel['title'])
        elif section_title != None:
            grafana_charts[section_title].append(
                {'title': panel['title'], 'description': panel['description'], 'type': panel['type']})
    return grafana_charts


def generate_markdown_table(section_title, charts):
    markdown_content = f"## {section_title}\n\n"
    if charts:
        for chart in charts:
            markdown_content += f"- **{chart['title']}**:\n\n"
            markdown_content += f"\tType: {chart['type']}\n\n"
            description = chart['description'].strip()
            if description:
                markdown_content += f"\t{description}\n\n"
    else:
        markdown_content += "No charts found in this section.\n"
    return markdown_content


def write_grafana_dashboard_md(file_path: str, output_file_path: str):
    grafana_charts = parse_dashboard_json(file_path)

    # Generate Markdown content
    markdown_content = ""
    for section, charts in grafana_charts.items():
        markdown_content += generate_markdown_table(section, charts)

    # Write Markdown content to file
    with open(output_file_path, 'w') as output_file:
        output_file.write(markdown_content)


if __name__ == "__main__":
    write_grafana_dashboard_md(
        './risingwave-user-dashboard.json', "./user-dashboard-doc.md")
    write_grafana_dashboard_md(
        './risingwave-dev-dashboard.json', "./dev-dashboard-doc.md")
