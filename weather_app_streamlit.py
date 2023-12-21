import streamlit as st
import pandas as pd
import pickle
import streamlit.components.v1 as components
from PIL import Image

# Load your pre-trained model
with open('random_forest_model.pkl', 'rb') as file:
    model = pickle.load(file)

# Function to make predictions


def make_prediction(input_data):
    return model.predict(pd.DataFrame([input_data]))

# Streamlit app


def main():
    st.title("Weather Prediction App")

    # Input fields

    tempmin = st.number_input('Min Temperature', value=10.0)

    feelslikemax = st.number_input('Feels Like Max Temperature', value=14.0)
    feelslikemin = st.number_input('Feels Like Min Temperature', value=9.0)

    humidity = st.number_input('Humidity', value=80.0)
    precip = st.number_input('Precipitation', value=5.0)

    # Creating a dictionary of inputs
    input_data = {

        'tempmin': tempmin,

        'feelslikemax': feelslikemax,
        'feelslikemin': feelslikemin,


        'humidity': humidity,
        'precip': precip

    }

    # Prediction
    if st.button('Predict'):
        prediction = make_prediction(input_data)
        class_mapping = {0: 'Clear', 1: 'Cloudy', 2: 'Rain', 3: 'Snow'}
        prediction = [class_mapping[label] for label in prediction]
        st.success(f' The prediction is: {prediction[0]}')

    st.title("Streamlit App with Tableau Dashboard")

    d1 = Image.open("Dashboard1.jpg")
    st.image(d1, width=1000)
    d2 = Image.open("Dashboard2.jpg")

    st.image(d2, width=1000)
    d3 = Image.open("Dashboard3.jpg")

    st.image(d3, width=1000)

    # tableau_url = "https://public.tableau.com/app/profile/sanyukta.nair/viz/Boulderweatherdata2022/Dashboard2022"

    # st.title("Streamlit App with Tableau Dashboard")

    # components.iframe(tableau_url, width=700, height=800)


if __name__ == "__main__":
    main()
