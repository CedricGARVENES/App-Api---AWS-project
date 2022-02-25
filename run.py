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


class TransfersDataForm(FlaskForm):
    submitRDStoS3 = SubmitField('Transférer de RDS à S3')
    submitS3toRDS = SubmitField('Transférer de S3 à RDS')


@app.route('/', methods=['GET', 'POST'])
@app.route('/home', methods=['GET', 'POST'])
def home():
    form = LoadDataForm()
    form2 = TransfersDataForm()

    if form.validate_on_submit():
        flash("La matière {name} a été envoyé dans {button}".format(
            name=form.name.data,
            button="RDS" if form.submitRDS.data else "S3"
        ), 'success')
        if form.submitRDS.data:
            requests_post_load(form, "RDS")
        else:
            requests_post_load(form, "S3")

    if form2.validate_on_submit():
        if form2.submitRDStoS3.data:
            requests_post_transfers("RDStoS3")
        else:
            requests_post_transfers("S3toRDS")

        return redirect(url_for('home'))

    return render_template("AppAWS.html", form=form, form2=form2)


def requests_post_load(form, way):
    # ToDo
    requests.post(url="http://X:80/transfers", data=json.dumps({'way': way,
                                                                   'name': form.name.data,
                                                                   'description': form.description.data,
                                                                   'hours': form.hours.data}))


def requests_post_transfers(way):
    # ToDo
    requests.post(url="http://X:80/transfers", data=json.dumps({'way': way}))


if __name__ == '__main__':
    app.run(debug=True, host="0.0.0.0", port=80)
