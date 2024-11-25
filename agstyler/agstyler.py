# Author: Nikolai Riabykh
# Original Source: https://github.com/nryabykh/streamlit-aggrid-hints/blob/master/src/agstyler.py

from st_aggrid import AgGrid
from st_aggrid.grid_options_builder import GridOptionsBuilder
from st_aggrid.shared import GridUpdateMode, JsCode

MAX_TABLE_HEIGHT = 300


def get_numeric_style_with_precision(precision: int) -> dict:
    return {"type": ["numericColumn", "customNumericFormat"], "precision": precision}


PRECISION_ZERO = get_numeric_style_with_precision(0)
PRECISION_ONE = get_numeric_style_with_precision(1)
PRECISION_TWO = get_numeric_style_with_precision(2)
PINLEFT = {"pinned": "left"}


def draw_grid(
        df,
        formatter: dict = None,
        selection="multiple",
        use_checkbox=False,
        fit_columns=False,
        theme="streamlit",
        max_height: int = MAX_TABLE_HEIGHT,
        max_width_pct: float = 0.8,
        wrap_text: bool = False,
        auto_height: bool = False,
        grid_options: dict = None,
        key=None,
        css: dict = None,
        max_row_height=120
):
    # Truncate text for relevant columns
    def truncate_text(text, max_length=100):
        if len(str(text)) > max_length:
            return str(text)[:max_length] + "..."
        return text

    # Apply truncation to columns
    for column in df.columns:
        if df[column].dtype == "object":  # Apply to string columns
            df[column] = df[column].apply(truncate_text)

    gb = GridOptionsBuilder()
    gb.configure_default_column(
        filterable=True,
        groupable=False,
        editable=False,
        wrapText=wrap_text,
        autoHeight=auto_height
    )

    if grid_options is not None:
        gb.configure_grid_options(**grid_options)

    for latin_name, (cyr_name, style_dict) in formatter.items():
        gb.configure_column(latin_name, header_name=cyr_name, **style_dict)

    gb.configure_selection(selection_mode=selection, use_checkbox=use_checkbox)

    # Ensure CSS enables truncation and scrolling
    if not css:
        css = {
            ".ag-cell": {
                "max-height": f"{max_row_height}px",  # Set maximum height
                "overflow": "hidden",  # Hide overflowing text
                "text-overflow": "ellipsis",  # Add ellipsis
                "word-break":"normal", # Natural word breaking
                "overflow-wrap": "break-word",  # Prevent splitting words
                "white-space": "normal",  # Enable wrapping
                "display": "-webkit-box",  # Multi-line truncation
                "-webkit-line-clamp": "2",  # Limit to 2 lines
                "-webkit-box-orient": "vertical",  # Required for line clamp
            },
            "#gridToolBar": {
            "padding-bottom": "0px !important",
            },
            ".ag-body-viewport": {
                "overflow-y": "auto",  # Enable vertical scrolling
                "overflow-x": "auto",  # Enable horizontal scrolling
                "max-height": f"{max_height-20}px",      # Ensure it matches the grid height
                # "max-width": "800px",  # Max width as a percentage of the container
                "padding-bottom": "25px",  # Extra space to prevent clipping
                
                # "max-height": f"{max_height}px"  # Limit table height
            },
        ".ag-root": {
            # "max-width": f"800px",  # Limit table width
            "width": "100%",  # Ensure the table fits within the container
            "height": f"{max_height}px",  # Set consistent height
        },
        ".ag-root-wrapper": {
            "overflow": "hidden",  # Prevent AG Grid's internal scrollbars
        },
        ".ag-theme-streamlit": {
            "margin-bottom": "20px",   # Ensure space around the table
            }
        }

    return AgGrid(
        df,
        gridOptions=gb.build(),
        update_mode=GridUpdateMode.SELECTION_CHANGED | GridUpdateMode.VALUE_CHANGED,
        allow_unsafe_jscode=True,
        fit_columns_on_grid_load=fit_columns,
        # height=min(max_height, (1 + len(df.index)) * 32 + 200),
        height=300,
        theme=theme,
        key=key,
        custom_css=css
    )


def highlight(color, condition):
    code = f"""
        function(params) {{
            color = "{color}";
            if ({condition}) {{
                return {{
                    'backgroundColor': color
                }}
            }}
        }};
    """
    return JsCode(code)


# def highlight_mult_colors(primary_color, secondary_color, condition):
#     code = f"""
#         function(params) {{
#             var primaryColor = "{primary_color}";
#             var secondaryColor = "{secondary_color}";
#             if ({condition}) {{
#                 return {{
#                     'backgroundColor': primaryColor
#                 }};
#             }} else {{
#                 return {{
#                     'backgroundColor': secondaryColor
#                 }};
#             }}
#         }}
#     """
#     return JsCode(code)

# following added by Reid Bongard to account for specific highlighting conditions
def highlight_mult_colors(primary_condition, primary_color, secondary_condition, secondary_color, final_color, font_size):
    code = f"""
        function(params) {{
            var primaryColor = "{primary_color}";
            var secondaryColor = "{secondary_color}";
            var finalColor = "{final_color}";
            if ({primary_condition}) {{
                return {{
                    'backgroundColor': primaryColor,
                    'fontSize': '{font_size}px'
                }};
            }} else if ({secondary_condition}) {{
                return {{
                    'backgroundColor': secondaryColor,
                    'fontSize': '{font_size}px'
                }};
            }} else {{
                return {{
                    'backgroundColor': finalColor,
                    'fontSize': '{font_size}px'
                }};
            }}
        }}
    """
    return JsCode(code)



# following added by Reid Bongard to account for specific highlighting conditions
# def highlight_mult_condition(primary_color, secondary_color, condition, secondary_condition):
#     code = f"""
#         function(params) {{
#             var primaryColor = "{primary_color}";
#             var secondaryColor = "{secondary_color}";
#             if ({condition}) {{
#                 if ({secondary_condition}) {{
#                     return {{'backgroundColor': primaryColor}};
#                 }} else {{
#                     return {{'backgroundColor': secondaryColor}};
#                 }}
#             }}
#         }}
#     """
#     return JsCode(code)

def highlight_mult_condition(conditions_colors, fallback_color):
    """
    conditions_colors: A list of tuples where each tuple contains a condition (str) and corresponding color (str).
    Example: [("params.data.vote === 'not_voting'", "red"), ("params.data.vote === 'abstain'", "yellow")]

    fallback_color: The background color to use if none of the conditions are met.
    """
    code = """
        function(params) {{
            """
    # Define color variables
    for num, (condition, color) in enumerate(conditions_colors):
        code += f"""
            var Color{num} = "{color}";
        """
    
    # Add conditions to check each color
    for num, (condition, _) in enumerate(conditions_colors):
        code += f"""
            if ({condition}) {{
                return {{'backgroundColor': Color{num}}};
            }}
        """
    
    # Add the fallback color in the else block
    code += f"""
            else {{
                return {{'backgroundColor': '{fallback_color}'}};
            }}
        }}
    """
    return JsCode(code)



# following added by Reid Bongard to link text to a url in a table
cellRenderer = JsCode("""
                                class UrlCellRenderer {
                                init(params) {
                                    this.eGui = document.createElement('a');
                                    this.eGui.innerText = params.value;
                                    this.eGui.setAttribute('href', params.data.url);
                                    this.eGui.setAttribute('style', "text-decoration:none");
                                    this.eGui.setAttribute('target', "_blank");
                                }
                                getGui() {
                                    return this.eGui;
                                }
                                }
                            """)