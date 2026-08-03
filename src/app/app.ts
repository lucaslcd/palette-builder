import { Component, signal } from '@angular/core';
import { ColorWheel } from './components/color-wheel/color-wheel';
import { ColorForm } from './components/color-form/color-form';
import { MatTabsModule } from '@angular/material/tabs';
import { MatSelectModule } from '@angular/material/select';
import { form, FormField } from "@angular/forms/signals";
import { ColorStrips } from './components/color-strips/color-strips';

@Component({
  selector: 'app-root',
  imports: [ColorWheel, ColorForm, ColorStrips, MatTabsModule, MatSelectModule, FormField],
  templateUrl: './app.html',
  styleUrl: './app.css'
})
export class App {
  protected readonly title = signal('palette-builder');

  display = form(
    signal({
      color: 'wheel',
      form: 'edit'
    })
  )
}
