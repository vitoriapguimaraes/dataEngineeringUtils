import streamlit as st


def setup_sidebar():
    """
    Sets up the common sidebar elements for the application.
    Should be called at the beginning of each page.
    """
    st.sidebar.caption("Trabalho de github.com/vitoriapguimaraes")


def add_back_to_top():
    """
    Adds a floating 'Back to Top' button.
    Should be called at the beginning of the page.
    """
    # Inject CSS for the button
    st.markdown(
        """
        <style>
            .back-to-top {
                position: fixed;
                bottom: 20px;
                right: 20px;
                background-color: #007BFF;
                color: white !important;
                padding: 10px 15px;
                text-decoration: none;
                box-shadow: 0px 4px 6px rgba(0,0,0,0.1);
                z-index: 99999;
                font-size: 20px;
                transition: background-color 0.3s;
                display: flex;
                align-items: center;
                justify-content: center;
                width: 50px;
                height: 50px;
            }
            .back-to-top:hover {
                background-color: #0056b3;
                text-decoration: none;
                color: white !important;
            }
        </style>
        """,
        unsafe_allow_html=True,
    )

    # Inject the button linking to the top anchor
    st.markdown(
        """
        <a href="#top-anchor" class="back-to-top" title="Voltar ao Topo">
            ⬆
        </a>
        <div id="top-anchor"></div>
        """,
        unsafe_allow_html=True,
    )
