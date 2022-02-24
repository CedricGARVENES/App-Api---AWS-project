from flask import Flask, render_template, url_for, flash, redirect
from flask_wtf import FlaskForm
from wtforms import StringField, SubmitField, IntegerField
from wtforms.validators import DataRequired, Length
import requests
import json

app = Flask(__name__)

app.config['SECRET_KEY'] = '932c7541cdb7928e157ac33d2709596c'


class LoadDataForm(FlaskForm):
    name = StringField('Name', validators=[DataRequired(), Length(min=2, max=30)])
    description = StringField('Description', validators=[DataRequired(), Length(min=2, max=500)])
    hours = IntegerField('Nombre d\'heures', validators=[DataRequired()])
    submitRDS = SubmitField('Charger dans RDS')
    submitS3 = SubmitField('Charger dans S3')


@app.route('/', methods=['GET', 'POST'])
@app.route('/home', methods=['GET', 'POST'])
def home():
    form = LoadDataForm()
    if form.validate_on_submit():
        flash("La matière {name} a été envoyé dans {button}".format(
            name=form.name.data,
            button="RDS" if form.submitRDS.data else "S3"
        ), 'success')
        if form.submitRDS.data:
            # ToDo
            requests.post(url="http://XXXX:80/transfers", data=json.dumps({'way': 'RDS',
                                                                'name': form.name.data,
                                                                'description': form.description.data,
                                                                'hours': form.hours.data}))
        else:
            # ToDo
            requests.post(url="http://XXXX:80/transfers", data=json.dumps({'way': 'S3',
                                                                'name': form.name.data,
                                                                'description': form.description.data,
                                                                'hours': form.hours.data}))
        return redirect(url_for('home'))
    return render_template("AppAWS.html", form=form)


if __name__ == '__main__':
    app.run(debug=True, host="0.0.0.0", port=80)
