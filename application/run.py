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
    submitRDStoS3 = SubmitField('Transférer de RDS à S3')
    submitS3toRDS = SubmitField('Transférer de S3 à RDS')


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
            requests_pots(form, "RDS")
        elif form.submitS3.data:
            requests_pots(form, "S3")
        elif form.submitRDStoS3.data:
            requests_pots(form, "RDStoS3")
        else:
            requests_pots(form, "S3toRDS")

        return redirect(url_for('home'))
    return render_template("AppAWS.html", form=form)


def requests_pots(form, way):
    # ToDo
    requests.post(url="http://XXXX:80/transfers", data=json.dumps({'way': way,
                                                                   'name': form.name.data,
                                                                   'description': form.description.data,
                                                                   'hours': form.hours.data}))


if __name__ == '__main__':
    app.run(debug=True, host="0.0.0.0", port=80)
