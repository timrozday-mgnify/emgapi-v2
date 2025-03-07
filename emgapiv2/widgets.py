import json
from typing import List

from django.forms import Widget
from django.utils.safestring import mark_safe


class StatusPathwayWidget(Widget):
    def __init__(self, attrs=None, pathway: List[str] = None):
        super().__init__(attrs)
        self.pathway = pathway if pathway else []

    def render(self, name, value, attrs=None, renderer=None):
        if value is None:
            value = {}
        else:
            value = json.loads(value)
        value = value or {}

        css = """
        <style>
            .vertical-pathway {
                display: flex;
                flex-direction: column;
                align-items: center;
                position: relative;
                padding: 20px;
            }

            .path-item {
                display: flex;
                align-items: center;
                position: relative;
                width: 100%;
                margin-bottom: 10px;
                cursor: pointer;
            }

            .circle {
                width: 20px;
                height: 20px;
                border-radius: 50%;
                background-color: grey;
                z-index: 2;
            }

            .green .circle {
                background-color: #18974c;
            }

            .red .circle {
                background-color: #d32f2f;
            }

            .line {
                position: absolute;
                left: 9px;
                width: 2px;
                height: 40px;
                background-color: grey;
                z-index: 1;
            }

            .path-item:first-child .line {
                top: 10px;
                height: 20px;
            }

            .path-item:last-child .line {
                display: none;
            }

            .json-key {
                margin-left: 10px;
                font-size: 16px;
            }
        </style>
        """
        script = """
        <script>
            function clickHandler(event) {
                const element = event.currentTarget;
                let circle = element.querySelector('.circle');
                let isGreen = element.classList.contains('green');
                element.classList.toggle('green', !isGreen);
                element.classList.toggle('red', isGreen);

                const name = element.dataset.name;
                let hiddenInput = document.getElementById(`${name}-hidden`);
                let data = JSON.parse(hiddenInput.value);
                let key = element.dataset.key;
                data[key] = !isGreen;
                hiddenInput.value = JSON.stringify(data);
            };
            function attachStatusHandlers() {
                document.querySelectorAll('.path-item').forEach(function(element) {
                    element.removeEventListener('click', clickHandler);
                    element.addEventListener('click', clickHandler);
                });
            }
            document.addEventListener('DOMContentLoaded', attachStatusHandlers);
            document.addEventListener('htmx:afterSettle', attachStatusHandlers);  // after pagination change
        </script>
        """
        html = f'<div class="vertical-pathway" data-name="{name}" id="{name}-pathway">'
        for step in self.pathway:
            val = value.get(step, None)
            status_class = "green" if val else "red"
            html += f"""
                <div class="path-item {status_class}" data-key="{step}" data-name="{name}">
                    <div class="circle"></div>
                    <span class="json-key">{step}</span>
                    <div class="line"></div>
                </div>
            """
        html += "</div>"
        html += '<input type="hidden" name="{}" id="{}-hidden" value=\'{}\' />'.format(
            name, name, json.dumps(value)
        )
        return mark_safe(css + html + script)
