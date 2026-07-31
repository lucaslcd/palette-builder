import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ColorWheel } from './color-wheel';

describe('ColorWheel', () => {
  let component: ColorWheel;
  let fixture: ComponentFixture<ColorWheel>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [ColorWheel],
    }).compileComponents();

    fixture = TestBed.createComponent(ColorWheel);
    component = fixture.componentInstance;
    await fixture.whenStable();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
